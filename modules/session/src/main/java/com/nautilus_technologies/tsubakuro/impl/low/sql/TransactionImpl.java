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
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.tsurugidb.jogasaki.proto.SqlResponse.ResultOnly;

/**
 * Transaction type.
 */
public class TransactionImpl implements Transaction {

    static final Logger LOG = LoggerFactory.getLogger(TransactionImpl.class);

    private SessionLinkImpl sessionLinkImpl;
    private final SqlCommon.Transaction transaction;
    private boolean cleanuped;
    private long timeout;
    private TimeUnit unit;

    /**
     * Class constructor, called from  FutureTransactionImpl.
     @param transaction a handle for this transaction
     @param sessionLinkImpl the caller of this constructor
     */
    public TransactionImpl(SqlCommon.Transaction transaction, SessionLinkImpl sessionLinkImpl) {
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
        return sessionLinkImpl.send(SqlRequest.ExecuteStatement.newBuilder()
                .setTransactionHandle(transaction)
                .setSql(source));
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(@Nonnull String source) throws IOException {
        Objects.requireNonNull(source);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(SqlRequest.ExecuteQuery.newBuilder()
                .setTransactionHandle(transaction)
                .setSql(source));
        return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
    }

    @Override
    public FutureResponse<ResultOnly> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pb = SqlRequest.ExecutePreparedStatement.newBuilder()
            .setTransactionHandle(transaction)
            .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle());
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        return sessionLinkImpl.send(pb);
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pb = SqlRequest.ExecutePreparedQuery.newBuilder()
        .setTransactionHandle(transaction)
        .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle());
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        var pair = sessionLinkImpl.send(pb);
        return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
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
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Path directory) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(directory);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pb = SqlRequest.ExecuteDump.newBuilder()
            .setTransactionHandle(transaction)
            .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
            .setDirectory(directory.toString());
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        var pair = sessionLinkImpl.send(pb);
        return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
    }

    @Override
    public FutureResponse<ResultOnly> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Collection<? extends Path> files) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(files);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pb = SqlRequest.ExecuteLoad.newBuilder()
        .setTransactionHandle(transaction)
        .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
        .addAllFile(files.stream()
        .map(Path::toString)
        .collect(Collectors.toList()));
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        return sessionLinkImpl.send(pb);
    }

    @Override
    public FutureResponse<ResultOnly> commit(@Nonnull SqlRequest.CommitStatus status) throws IOException {
        Objects.requireNonNull(status);
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var rv = sessionLinkImpl.send(SqlRequest.Commit.newBuilder()
                .setTransactionHandle(transaction)
                .setNotificationType(status));
        cleanuped = true;
        dispose();
        return rv;
    }

    @Override
    public FutureResponse<SqlResponse.ResultOnly> rollback() throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var rv = submitRollback();
        cleanuped = true;
        dispose();
        return rv;
    }

    private FutureResponse<ResultOnly> submitRollback() throws IOException {
        var rv = sessionLinkImpl.send(SqlRequest.Rollback.newBuilder()
                .setTransactionHandle(transaction));
        return rv;
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * This is used when the transaction is to be closed without commit or rollback.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
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
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
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
