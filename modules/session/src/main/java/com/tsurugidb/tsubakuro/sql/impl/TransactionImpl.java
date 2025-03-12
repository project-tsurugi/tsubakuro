/*
 * Copyright 2023-2024 Project Tsurugi.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import com.tsurugidb.tsubakuro.sql.io.BlobException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * Transaction type.
 */
public class TransactionImpl implements Transaction {

    static final Logger LOG = LoggerFactory.getLogger(TransactionImpl.class);

    private final SqlResponse.Begin.Success transaction;
    private final AtomicBoolean cleanuped = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private long timeout = 0;
    private TimeUnit unit;
    private final SqlService service;
    private final ServerResource.CloseHandler closeHandler;
    private final boolean autoDispose = false;
    private boolean needDispose = true;

    private static AtomicLong blobNumber = new AtomicLong();
    private static AtomicLong clobNumber = new AtomicLong();

    /**
     * Creates a new instance.
     * @param transaction the SqlResponse.Begin.Success
     * @param service the SQL service
     * @param closeHandler handles {@link #close()} was invoked
     */
    public TransactionImpl(
            SqlResponse.Begin.Success transaction,
            @Nonnull SqlService service,
            @Nullable ServerResource.CloseHandler closeHandler) {
        Objects.requireNonNull(service);
        this.transaction = transaction;
        this.service = service;
        this.closeHandler = closeHandler;
    }

    @Override
    public FutureResponse<ExecuteResult> executeStatement(@Nonnull String source) throws IOException {
        Objects.requireNonNull(source);
        if (cleanuped.get()) {
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
        if (cleanuped.get()) {
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
        if (cleanuped.get()) {
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
        if (cleanuped.get()) {
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
                    channelName += Long.valueOf(clobNumber.getAndIncrement() + 1).toString();
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
                    channelName += Long.valueOf(blobNumber.getAndIncrement() + 1).toString();
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
    public FutureResponse<ResultSet> executeDump(@Nonnull String source, @Nonnull Path directory) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(directory);
        // FIXME impl
        throw new UnsupportedOperationException();
    }

    @Override
    public FutureResponse<ExecuteResult> batch(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends Collection<? extends SqlRequest.Parameter>> parameterTable)
                    throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameterTable);
        if (cleanuped.get()) {
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
        if (cleanuped.get()) {
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
        if (cleanuped.get()) {
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
    public FutureResponse<Void> commit(@Nonnull SqlRequest.CommitStatus status) throws IOException {
        Objects.requireNonNull(status);
        if (cleanuped.getAndSet(true)) {
            throw new IOException("transaction already closed");
        }
        if (autoDispose && (service instanceof SqlServiceStub)) {
            return ((SqlServiceStub) service).send(SqlRequest.Commit.newBuilder()
                    .setTransactionHandle(transaction.getTransactionHandle())
                    .setNotificationType(status)
                    .setAutoDispose(true)
                    .build(), this);
        }
        return service.send(SqlRequest.Commit.newBuilder()
                .setTransactionHandle(transaction.getTransactionHandle())
                .setNotificationType(status)
                .setAutoDispose(false)
                .build());
    }

    @Override
    public FutureResponse<Void> rollback() throws IOException {
        if (cleanuped.getAndSet(true)) {
            return FutureResponse.returns(null);
        }
        return submitRollback();
    }

    @Override
    public FutureResponse<SqlServiceException> getSqlServiceException() throws IOException {
        if (closed.get()) {
            throw new IOException("transaction already closed");
        }
        if (!needDispose) {
            return FutureResponse.returns(null);
        }
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
        timeout = t;
        unit = u;
    }

    @Override
    public String getTransactionId() {
        return transaction.getTransactionId().getId();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!closed.getAndSet(true)) {
            try {
                if (!cleanuped.getAndSet(true)) {
                    // FIXME need to consider rollback is suitable here
                    try (var rollback = submitRollback()) {
                        if (timeout == 0) {
                            rollback.get();
                        } else {
                            rollback.get(timeout, unit);
                        }
                    } catch (TimeoutException e) {
                        LOG.warn("timeout occurred in the transaction rollback", e);
                        throw new ResponseTimeoutException(e.getMessage(), e);
                    }
                }
            } finally {
                if (closeHandler != null) {
                    Lang.suppress(
                            e -> LOG.warn("error occurred while collecting garbage", e),
                            () -> closeHandler.onClosed(this));
                }
                if (needDispose) {
                    try (var futureResponse = service.send(SqlRequest.DisposeTransaction.newBuilder()
                            .setTransactionHandle(transaction.getTransactionHandle())
                            .build())) {
                        if (timeout == 0) {
                            futureResponse.get();
                        } else {
                            futureResponse.get(timeout, unit);
                        }
                    } catch (TimeoutException e) {
                        LOG.warn("timeout occurred in the transaction disposal", e);
                        throw new ResponseTimeoutException(e.getMessage(), e);
                    }
                }
            }
        }
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

    private FutureResponse<Void> submitRollback() throws IOException {
        return service.send(SqlRequest.Rollback.newBuilder()
                .setTransactionHandle(transaction.getTransactionHandle())
                .build());
    }

    void notifyCommitSuccess() {
        needDispose = false;
    }

    // for diagnostic
    String diagnosticInfo() {
        if (!closed.get()) {
            return " +Transaction (universal ID = " + transaction.getTransactionId().getId() + ", handle = " + transaction.getTransactionHandle().getHandle() + ")" + System.getProperty("line.separator");
        }
        return "";
    }
}
