/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.common.impl.FileBlobInfo;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.LargeObjectCache;
import com.tsurugidb.tsubakuro.sql.LargeObjectReference;
import com.tsurugidb.tsubakuro.sql.TransactionStatus;
import com.tsurugidb.tsubakuro.sql.io.BlobException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * Transaction type.
 */
public class TransactionImpl implements Transaction {

    static final Logger LOG = LoggerFactory.getLogger(TransactionImpl.class);

    private final SqlResponse.Begin.Success transaction;
    private Timeout timeout = null;
    private final SqlService service;
    private final ServerResource.CloseHandler closeHandler;
    private FutureResponse<Void> commitResult;
    private FutureResponse<Void> rollbackResult = null;
    private FutureResponse<Void> disposeResult = null;
    private int disposeRetry = 10;
    private int commitRetry = 10;
    private AtomicReference<State> state = new AtomicReference<>();
    private Disposer disposer = null;
    private boolean autoDispose = false;

    private enum State {
                                    // | commitResult | commit    | rollback  | delayedClose | Transaction |
                                    // |              | requested | requested | registerd    | closed      |
                                    // ---------------------------------------------------------------------
        INITIAL,                    // | null         | no        | no        | no           | no          |
        COMMITTED,                  // | not null     | yes       | no        | no           | no          |
        ROLLBACKED,                 // | null         | no        | yes       | no           | no          |
        TO_BE_CLOSED,               // | null         | no        | no        | yes          | no          |
        TO_BE_CLOSED_WITH_COMMIT,   // | not null     | yes       | no        | yes          | no          |
        TO_BE_CLOSED_WITH_ROLLBACK, // | null         | no        | yes       | yes          | no          |
        CLOSED                      // | -(don't care)| -         | -         | -            | yes         |
    }
    private boolean isCleanuped() {
        return state.get() != State.INITIAL;
    }

    private static AtomicLong blobNumber = new AtomicLong(1);
    private static AtomicLong clobNumber = new AtomicLong(1);

    /**
     * Creates a new instance.
     * @param transaction the SqlResponse.Begin.Success
     * @param service the SQL service
     * @param closeHandler handles {@link #close()} was invoked
     * @param disposer the Disposer in charge of its asynchronous close
     */
    public TransactionImpl(
            @Nonnull SqlResponse.Begin.Success transaction,
            @Nonnull SqlService service,
            @Nullable ServerResource.CloseHandler closeHandler,
            @Nullable Disposer disposer) {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(service);
        this.transaction = transaction;
        this.service = service;
        this.closeHandler = closeHandler;
        this.disposer = disposer;
        this.timeout = null;
        state.set(State.INITIAL);
    }

    @Override
    public FutureResponse<ExecuteResult> executeStatement(@Nonnull String source) throws IOException {
        Objects.requireNonNull(source);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        return service.send(SqlRequest.ExecuteStatement.newBuilder()
                            .setTransactionHandle(transaction.getTransactionHandle())
                .setSql(source)
                .build());
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(@Nonnull String source) throws IOException {
        Objects.requireNonNull(source);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        return service.send(SqlRequest.ExecuteQuery.newBuilder()
                .setTransactionHandle(transaction.getTransactionHandle())
                .setSql(source)
                .build());
    }

    @Override
    public FutureResponse<ExecuteResult> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        var pb = SqlRequest.ExecutePreparedStatement.newBuilder()
            .setTransactionHandle(transaction.getTransactionHandle())
            .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle());
        var lobs = new LinkedList<BlobInfo>();
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(addLob(e, lobs));
        }
        if (lobs.isEmpty()) {
            return service.send(pb.build());
        } else {
            return service.send(pb.build(), lobs);
        }
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        var pb = SqlRequest.ExecutePreparedQuery.newBuilder()
        .setTransactionHandle(transaction.getTransactionHandle())
        .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle());
        var lobs = new LinkedList<BlobInfo>();
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(addLob(e, lobs));
        }
        if (lobs.isEmpty()) {
            return service.send(pb.build());
        } else {
            return service.send(pb.build(), lobs);
        }
    }

    private SqlRequest.Parameter addLob(SqlRequest.Parameter e, LinkedList<BlobInfo> lobs) throws BlobException {
        if (e.getValueCase() == SqlRequest.Parameter.ValueCase.CLOB) {
            var v = e.getClob();
            switch (v.getDataCase()) {
                case LOCAL_PATH:
                    var path = Path.of(v.getLocalPath());
                    if (!Files.isReadable(path)) {
                        throw new BlobException("error occurred while transmitting BLOB data: {" + path + " is not readable}");
                    }
                    String channelName = "ClobChannel-";
                    channelName += Long.valueOf(ProcessHandle.current().pid()).toString();
                    channelName += "-";
                    channelName += Long.valueOf(clobNumber.getAndIncrement()).toString();
                    if (!lobs.add(new FileBlobInfo(channelName, path))) {
                        throw new IllegalArgumentException();
                    }
                    return SqlRequest.Parameter.newBuilder()
                            .setName(e.getName())
                            .setClob(SqlCommon.Clob.newBuilder()
                                    .setChannelName(channelName)
                                    .build())
                            .build();
                case CONTENTS:
                    return e;
                default:
                    throw new IllegalArgumentException();
            }
        } else if (e.getValueCase() == SqlRequest.Parameter.ValueCase.BLOB) {
            var v = e.getBlob();
            switch (v.getDataCase()) {
                case LOCAL_PATH:
                    var path = Path.of(v.getLocalPath());
                    if (!Files.isReadable(path)) {
                        throw new BlobException("error occurred while transmitting BLOB data: {" + path + " is not readable}");
                    }
                    String channelName = "BlobChannel-";
                    channelName += Long.valueOf(ProcessHandle.current().pid()).toString();
                    channelName += "-";
                    channelName += Long.valueOf(blobNumber.getAndIncrement()).toString();
                    if (!lobs.add(new FileBlobInfo(channelName, path))) {
                        throw new IllegalArgumentException();
                    }
                    return SqlRequest.Parameter.newBuilder()
                            .setName(e.getName())
                            .setBlob(SqlCommon.Blob.newBuilder()
                                    .setChannelName(channelName)
                                    .build())
                            .build();
                case CONTENTS:
                    return e;
                default:
                    throw new IllegalArgumentException();
            }
        }
        return e;
    }

    @Override
    public FutureResponse<ResultSet> executeDump(
            @Nonnull String source,
            @Nonnull Path directory) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(directory);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        var pb = SqlRequest.ExecuteDumpByText.newBuilder()
                .setTransactionHandle(transaction.getTransactionHandle())
                .setSql(source)
                .setDirectory(directory.toString())
                .setOption(SqlRequest.DumpOption.getDefaultInstance());
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<ExecuteResult> batch(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends Collection<? extends SqlRequest.Parameter>> parameterTable)
                    throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameterTable);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        var request = SqlRequest.Batch.newBuilder()
                .setTransactionHandle(transaction.getTransactionHandle())
                .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
                .addAllParameterSets(parameterTable.stream()
                        .map(it -> SqlRequest.ParameterSet.newBuilder()
                                .addAllElements(it)
                                .build())
                        .collect(Collectors.toList()))
                .build();
        return service.send(request);
    }

    @Override
    public FutureResponse<ResultSet> executeDump(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Path directory) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(directory);
        return executeDump(statement, parameters, directory, SqlRequest.DumpOption.getDefaultInstance());
    }

    @Override
    public FutureResponse<ResultSet> executeDump(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Path directory,
            @Nonnull SqlRequest.DumpOption option) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(directory);
        Objects.requireNonNull(option);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        var pb = SqlRequest.ExecuteDump.newBuilder()
                .setTransactionHandle(transaction.getTransactionHandle())
                .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
                .setDirectory(directory.toString());
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        pb.setOption(option);
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<ExecuteResult> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Collection<? extends Path> files) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(files);
        if (isCleanuped()) {
            throw new IOException("transaction already closed");
        }
        var pb = SqlRequest.ExecuteLoad.newBuilder()
        .setTransactionHandle(transaction.getTransactionHandle())
        .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
        .addAllFile(files.stream()
        .map(Path::toString)
        .collect(Collectors.toList()));
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<TransactionStatus.TransactionStatusWithMessage> getStatus() throws IOException {
        var pb = SqlRequest.GetTransactionStatus.newBuilder()
                    .setTransactionHandle(transaction.getTransactionHandle());
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<Void> commit(@Nonnull SqlRequest.CommitStatus status) throws IOException {
        Objects.requireNonNull(status);

        return commit(SqlRequest.CommitOption.newBuilder().setNotificationType(status).build());
    }

    @Override
    public synchronized FutureResponse<Void> commit(@Nonnull SqlRequest.CommitOption option) throws IOException {
        Objects.requireNonNull(option);

        switch (state.get()) {
            case INITIAL:
                autoDispose = option.getAutoDispose();
                commitResult = service.send(SqlRequest.Commit.newBuilder()
                                    .setTransactionHandle(transaction.getTransactionHandle())
                                    .setNotificationType(option.getNotificationType())
                                    .setAutoDispose(autoDispose)
                                    .setOption(option)
                                    .build());
                state.set(State.COMMITTED);
                return commitResult;
            case COMMITTED:
                return commitResult;
            case ROLLBACKED:
                throw new IOException("transaction already rollbacked");
            default:
                throw new IOException("transaction already closed");
        }
    }

    @Override
    public synchronized FutureResponse<Void> rollback() throws IOException {
        switch (state.get()) {
            case INITIAL:
                state.set(State.ROLLBACKED);
                return submitRollback();
            case COMMITTED:
                if (commitResult.isDone()) {
                    try {
                        commitResult.get();
                        throw new IOException("transaction already committed and succeeded");
                    } catch (IOException | ServerException | InterruptedException e) {
                        return submitRollback();
                    }
                }
                throw new IOException("transaction already committed");
            case ROLLBACKED:
                return FutureResponse.returns(null);
            default:
                throw new IOException("transaction already closed");
        }
    }

    @Override
    public synchronized FutureResponse<SqlServiceException> getSqlServiceException() throws IOException {
        if (state.get() == State.CLOSED) {
            throw new IOException("transaction already closed");
        }
        var cr = commitResult;
        if (cr != null && cr.isDone()) {
            try {
                cr.get();
                return FutureResponse.returns(null);
            } catch (IOException | ServerException | InterruptedException e) {
                return sendAndGetSqlServiceException();
            }
        }
        return sendAndGetSqlServiceException();
    }
    private FutureResponse<SqlServiceException> sendAndGetSqlServiceException() throws IOException {
        return service.send(SqlRequest.GetErrorInfo.newBuilder()
                .setTransactionHandle(transaction.getTransactionHandle())
                .build());
    }

    @Override
    public FutureResponse<InputStream> openInputStream(@Nonnull BlobReference ref) throws IOException {
        Objects.requireNonNull(ref);
        if (ref instanceof BlobReferenceForSql) {
            var blobReferenceForSql = (BlobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setTransactionHandle(transaction.getTransactionHandle())
                        .setReference(blobReferenceForSql.blobReference());
            return service.send(pb.build(), ref);
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<Reader> openReader(@Nonnull ClobReference ref) throws IOException {
        Objects.requireNonNull(ref);
        if (ref instanceof ClobReferenceForSql) {
            var clobReferenceForSql = (ClobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setTransactionHandle(transaction.getTransactionHandle())
                        .setReference(clobReferenceForSql.clobReference());
            return service.send(pb.build(), ref);
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<LargeObjectCache> getLargeObjectCache(@Nonnull LargeObjectReference ref) throws IOException {
        Objects.requireNonNull(ref);
        var pb = SqlRequest.GetLargeObjectData.newBuilder()
                    .setTransactionHandle(transaction.getTransactionHandle());
        if (ref instanceof BlobReferenceForSql) {
            pb.setReference(((BlobReferenceForSql) ref).blobReference());
            return service.send(pb.build());
        } else if (ref instanceof ClobReferenceForSql) {
            pb.setReference(((ClobReferenceForSql) ref).clobReference());
            return service.send(pb.build());
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<Void> copyTo(@Nonnull LargeObjectReference ref, @Nonnull Path destination) throws IOException {
        Objects.requireNonNull(ref);
        var pb = SqlRequest.GetLargeObjectData.newBuilder()
                    .setTransactionHandle(transaction.getTransactionHandle());
        if (ref instanceof BlobReferenceForSql) {
            pb.setReference(((BlobReferenceForSql) ref).blobReference());
            return service.send(pb.build(), destination);
        } else if (ref instanceof ClobReferenceForSql) {
            pb.setReference(((ClobReferenceForSql) ref).clobReference());
            return service.send(pb.build(), destination);
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public void setCloseTimeout(long t, TimeUnit u) {
        synchronized (this) {
            if (t != 0 && u != null) {
                timeout = new Timeout(t, u, Timeout.Policy.ERROR);
                return;
            }
            timeout = null;
        }
    }

    @Override
    public String getTransactionId() {
        return transaction.getTransactionId().getId();
    }

    @Override
    public synchronized void close() throws IOException, ServerException, InterruptedException {
        var s = state.get();
        switch (s) {
        case INITIAL:
            // FIXME need to consider rollback is suitable here
            submitRollback();
            submitDisposeRequest();
            break;
        case ROLLBACKED:
            submitDisposeRequest();
            break;
        case COMMITTED:
            if (commitResult.isDone()) {
                if (doClose()) {
                    return;
                }
            }
            break;
        case TO_BE_CLOSED:
        case TO_BE_CLOSED_WITH_COMMIT:
        case TO_BE_CLOSED_WITH_ROLLBACK:
        case CLOSED:
            return;
        }

        if (disposer != null) {
            if (disposer.isClosingNow()) {
                doClose();
            } else {
                state.set(toBeClosed(s));
                disposer.add(new Disposer.DelayedClose() {
                    @Override
                    public boolean delayedClose() throws ServerException, IOException, InterruptedException {
                        return doClose();
                    }
                });
            }
            return;
        }
        doClose();
    }

    private FutureResponse<Void> submitRollback() throws IOException {
        if (rollbackResult == null) {
            rollbackResult = service.send(SqlRequest.Rollback.newBuilder()
                                .setTransactionHandle(transaction.getTransactionHandle())
                                .build());
        }
        return rollbackResult;
    }

    private void submitDisposeRequest() throws IOException {
        if (disposeResult == null) {
            disposeResult = service.send(SqlRequest.DisposeTransaction.newBuilder()
                                .setTransactionHandle(transaction.getTransactionHandle())
                                .build());
        }
    }

    private State toBeClosed(State s) {
        switch (s) {
        case INITIAL:
            return State.TO_BE_CLOSED;
        case COMMITTED:
            return State.TO_BE_CLOSED_WITH_COMMIT;
        case ROLLBACKED:
            return State.TO_BE_CLOSED_WITH_ROLLBACK;
        default:
            throw new AssertionError("improper state given, state: " + s);
        }
    }

    private synchronized boolean doClose() throws IOException, ServerException, InterruptedException {
        boolean needDispose = false;

        switch (state.get()) {
        case INITIAL:
        case TO_BE_CLOSED:
            break;
        case COMMITTED:
        case TO_BE_CLOSED_WITH_COMMIT:
            try {
                commitResult.get(1000, TimeUnit.MICROSECONDS);
                if (!autoDispose) {
                    needDispose = true;
                }
            } catch (ResponseTimeoutException | TimeoutException e) {
                if (--commitRetry > 0 || timeout == null) {
                    return false;
                }
                timeout.waitFor(commitResult);
                return false;
            } catch (IOException | ServerException | InterruptedException e) {
                needDispose = true;
            }
            break;
        case ROLLBACKED:
        case TO_BE_CLOSED_WITH_ROLLBACK:
            break;
        case CLOSED:
            if (careDisposeResult()) {
                return true;
            }
            if (--disposeRetry > 0) {
                return false;
            }
            throw new IOException("server does not reply the dispose request and give up waiting for reply");
        }
        if (needDispose) {
            submitDisposeRequest();
        }
        try {
            if (rollbackResult != null) {
                try {
                    rollbackResult.get(1000, TimeUnit.MICROSECONDS);
                } catch (ResponseTimeoutException | TimeoutException e) {
                    return false;
                }
            }
            return careDisposeResult();
        } finally {
            state.set(State.CLOSED);
        }
    }

    private boolean careDisposeResult() throws IOException, ServerException, InterruptedException {
        if (disposeResult != null) {
            try {
                if (timeout == null) {
                    disposeResult.get(1000, TimeUnit.MICROSECONDS);
                } else {
                    timeout.waitFor(disposeResult);
                }
            } catch (ResponseTimeoutException | TimeoutException e) {
                    return false;
            }
            disposeResult = null;
        }
        if (closeHandler != null) {
            Lang.suppress(
                e -> LOG.warn("error occurred while collecting garbage", e),
                () -> closeHandler.onClosed(this)
            );
        }
        return true;
    }

    /**
     * Extracts transaction ID.
     * @param transaction the target transaction
     * @return the transaction ID, or empty if extraction is failed
     */
    public static OptionalLong getId(@Nullable Transaction transaction) {
        if (transaction instanceof TransactionImpl) {
            return OptionalLong.of(((TransactionImpl) transaction).transaction.getTransactionHandle().getHandle());
        }
        return OptionalLong.empty();
    }

    // for diagnostic
    String diagnosticInfo() {
        if (state.get() != State.CLOSED) {
            return " +Transaction (universal ID = " + transaction.getTransactionId().getId() + ", handle = " + transaction.getTransactionHandle().getHandle() + ")" + System.lineSeparator();
        }
        return "";
    }
}
