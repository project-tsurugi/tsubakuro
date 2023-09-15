package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
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
    private final boolean autoDispose = true;
    private boolean needDispose = true;

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
        this.timeout = 0;
    }

    @Override
    public FutureResponse<Void> executeStatement(@Nonnull String source) throws IOException {
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
    public FutureResponse<Void> executeStatement(
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
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        return service.send(pb.build());
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
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<ResultSet> executeDump(@Nonnull String source, @Nonnull Path directory) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(directory);
        // FIXME impl
        throw new UnsupportedOperationException();
    }

    @Override
    public FutureResponse<Void> batch(
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
    public FutureResponse<Void> executeLoad(
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
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
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
