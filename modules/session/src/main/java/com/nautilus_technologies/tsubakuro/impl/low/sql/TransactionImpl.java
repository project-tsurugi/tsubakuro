package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute statement by the SQL service
     */
    @Override
    public FutureResponse<ResponseProtos.ResultOnly> executeStatement(String sql) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        return sessionLinkImpl.send(RequestProtos.ExecuteStatement.newBuilder()
                .setTransactionHandle(transaction)
                .setSql(sql));
    }

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return a FutureResponse of ResultSet which is a processing result of the SQL service
     * @throws IOException error occurred in execute query by the SQL service
     */
    @Override
    public FutureResponse<ResultSet> executeQuery(String sql) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(RequestProtos.ExecuteQuery.newBuilder()
                .setTransactionHandle(transaction)
                .setSql(sql));
        if (!Objects.isNull(pair.getLeft())) {
            return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
        }
        return new FutureResultSetImpl(pair.getRight());
    };

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute statement by the SQL service
     */
    @Override
    public FutureResponse<ResponseProtos.ResultOnly> executeStatement(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        return sessionLinkImpl.send(RequestProtos.ExecutePreparedStatement.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
                .setParameters(parameterSet));
    }
    @Override
    @Deprecated
    public FutureResponse<ResponseProtos.ResultOnly> executeStatement(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        return sessionLinkImpl.send(RequestProtos.ExecutePreparedStatement.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
                .setParameters(parameterSet));
    }

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a FutureResponse of ResultSet which is a processing result of the SQL service
     * @throws IOException error occurred in execute query by the SQL service
     */
    @Override
    public FutureResponse<ResultSet> executeQuery(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(RequestProtos.ExecutePreparedQuery.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
                .setParameters(parameterSet));
        if (!Objects.isNull(pair.getLeft())) {
            return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
        }
        return new FutureResultSetImpl(pair.getRight());
    }
    @Override
    @Deprecated
    public FutureResponse<ResultSet> executeQuery(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(RequestProtos.ExecutePreparedQuery.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
                .setParameters(parameterSet));
        if (!Objects.isNull(pair.getLeft())) {
            return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
        }
        return new FutureResultSetImpl(pair.getRight());
    }

    /**
     * Request commit to the SQL service
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in commit by the SQL service
     */
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

    /**
     * Request rollback to the SQL service
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in rollback by the SQL service
     */
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
     * Request dump execution to the SQL service
     * @param preparedStatement prepared statement used in the dump operation
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @param directory the directory path where dumped files are placed
     * @return a FutureResponse of ResultSet which is a processing result of the SQL service
     * @throws IOException error occurred in execute dump by the SQL service
     */
    @Override
    public FutureResponse<ResultSet> executeDump(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet, Path directory) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var pair = sessionLinkImpl.send(RequestProtos.ExecuteDump.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
                .setParameters(parameterSet)
                .setDirectory(directory.toString()));
        if (!Objects.isNull(pair.getLeft())) {
            return new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl, pair.getRight());
        }
        return new FutureResultSetImpl(pair.getRight());
    }

    /**
     * Request load execution to the SQL service
     * @param preparedStatement prepared statement used in the dump operation
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @param files the collection of file path to be loaded
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute load by the SQL service
     */
    @Override
    public FutureResponse<ResponseProtos.ResultOnly> executeLoad(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet, Collection<? extends Path> files) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("already closed");
        }
        var message = RequestProtos.ExecuteLoad.newBuilder()
                .setTransactionHandle(transaction)
                .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
                .setParameters(parameterSet);
        for (Path file : files) {
            message.addFile(file.toString());
        }
        return sessionLinkImpl.send(message);
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
