package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * Transaction type.
 */
public class TransactionImpl implements Transaction {

    static final Logger LOG = LoggerFactory.getLogger(TransactionImpl.class);

    private SessionLinkImpl sessionLinkImpl;
    private final CommonProtos.Transaction transaction;
    private boolean cleanuped;
    private long timeout;
    private TimeUnit unit;

    /**
     * Class constructor, called from  FutureTransactionImpl.
     @param transaction a handle for this transaction
     @param sessionLinkImpl the caller of this constructor
     */
    public TransactionImpl(CommonProtos.Transaction transaction, SessionLinkImpl sessionLinkImpl) {
        this.sessionLinkImpl = sessionLinkImpl;
        this.transaction = transaction;
        this.sessionLinkImpl.add(this);
        this.cleanuped = false;
        this.timeout = 0;
    }

    @Override
    public FutureResponse<ResultOnly> executeStatement(@Nonnull String source) throws IOException {
        Objects.requireNonNull(source);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        return sessionLinkImpl.send(RequestProtos.ExecuteStatement.newBuilder()
                .setTransactionHandle(transaction)
                .setSql(source));
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(@Nonnull String source) throws IOException {
        Objects.requireNonNull(source);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(RequestProtos.ExecuteQuery.newBuilder()
                .setTransactionHandle(transaction)
                .setSql(source));
        if (!Objects.isNull(pair.getLeft())) {
            return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
        }
        return new FutureResultSetImpl(pair.getRight());
    }

    @Override
    public FutureResponse<ResultOnly> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        return sessionLinkImpl.send(RequestProtos.ExecutePreparedStatement.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
                .setParameters(RequestProtos.ParameterSet.newBuilder()
                        .addAllParameters(parameters)));
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(RequestProtos.ExecutePreparedQuery.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
                .setParameters(RequestProtos.ParameterSet.newBuilder()
                        .addAllParameters(parameters)));
        if (!Objects.isNull(pair.getLeft())) {
            return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
        }
        return new FutureResultSetImpl(pair.getRight());
    }

    @Override
    public FutureResponse<ResultSet> executeDump(@Nonnull String source, @Nonnull Path directory) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(directory);
        // FIXME impl
        throw new UnsupportedOperationException();
    }

    @Override
    public FutureResponse<ResultSet> executeDump(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters,
            @Nonnull Path directory) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(directory);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(RequestProtos.ExecuteDump.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
                .setParameters(RequestProtos.ParameterSet.newBuilder()
                        .addAllParameters(parameters))
                .setDirectory(directory.toString()));
        if (!Objects.isNull(pair.getLeft())) {
            return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
        }
        return new FutureResultSetImpl(pair.getRight());
    }

    @Override
    public FutureResponse<ResultOnly> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters,
            @Nonnull Collection<? extends Path> files) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(files);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        return sessionLinkImpl.send(RequestProtos.ExecuteLoad.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
                .setParameters(RequestProtos.ParameterSet.newBuilder()
                        .addAllParameters(parameters))
                .addAllFile(files.stream()
                        .map(Path::toString)
                        .collect(Collectors.toList())));
    }

    @Override
    public FutureResponse<ResponseProtos.ResultOnly> commit() throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var rv = sessionLinkImpl.send(RequestProtos.Commit.newBuilder()
                .setTransactionHandle(transaction));
        cleanuped = true;
        dispose();
        return rv;
    }

    @Override
    public FutureResponse<ResponseProtos.ResultOnly> rollback() throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var rv = submitRollback();
        cleanuped = true;
        dispose();
        return rv;
    }

    private FutureResponse<ResultOnly> submitRollback() throws IOException {
        var rv = sessionLinkImpl.send(RequestProtos.Rollback.newBuilder()
                .setTransactionHandle(transaction));
        return rv;
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * This is used when the transaction is to be closed without commit or rollback.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
    @Override
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (Objects.nonNull(sessionLinkImpl)) {
            if (!cleanuped) {
                // FIXME need to consider rollback is suitable here
                try (var rollback = submitRollback()) {
                    var response = (timeout == 0) ? rollback.get() : rollback.get(timeout, unit);
                    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
                        throw new IOException(response.getError().getDetail());
                    }
                } catch (TimeoutException e) {
                    LOG.warn("disposing transaction is timeout", e);
                }
            }
            dispose();
        }
    }

    private void dispose() {
        sessionLinkImpl.remove(this);
        sessionLinkImpl = null;
    }
}
