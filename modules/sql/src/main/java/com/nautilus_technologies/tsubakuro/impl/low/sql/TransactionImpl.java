package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * Transaction type.
 */
public class TransactionImpl implements Transaction {
    private SessionLinkImpl sessionLinkImpl;
    private CommonProtos.Transaction transaction;
    private boolean cleanuped;
    private long timeout;
    private TimeUnit unit;

    /**
     * Class constructor, called from  FutureTransactionImpl.
     @param wire the wire responsible for the communication conducted by this session
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
     @param sql sql text for the command
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> executeStatement(String sql) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("already closed");
	}
	return sessionLinkImpl.send(RequestProtos.ExecuteStatement.newBuilder()
				.setTransactionHandle(transaction)
				.setSql(sql));
    }

    /**
     * Request executeQuery to the SQL service
     @param sql sql text for the command
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire and record metadata,
     and Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not.
     */
    public Pair<Future<ResultSet>, Future<ResponseProtos.ResultOnly>> executeQuery(String sql) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("already closed");
	}
	var pair = sessionLinkImpl.send(RequestProtos.ExecuteQuery.newBuilder()
					.setTransactionHandle(transaction)
					.setSql(sql));
	if (!Objects.isNull(pair.getLeft())) {
	    return Pair.of(new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl), pair.getRight());
	}
	return Pair.of((FutureResultSetImpl) null, pair.getRight());
    };

    /**
     * Request executeStatement to the SQL service
     @param preparedStatement prepared statement for the command
     @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> executeStatement(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
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
     @param preparedStatement prepared statement for the command
     @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire and record metadata,
     and Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not.
     */
    public Pair<Future<ResultSet>, Future<ResponseProtos.ResultOnly>> executeQuery(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("already closed");
	}
	var pair = sessionLinkImpl.send(RequestProtos.ExecutePreparedQuery.newBuilder()
					.setTransactionHandle(transaction)
					.setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
					.setParameters(parameterSet));
	if (!Objects.isNull(pair.getLeft())) {
	    return Pair.of(new FutureResultSetImpl(pair.getLeft(), sessionLinkImpl), pair.getRight());
	}
	return Pair.of((FutureResultSetImpl) null, pair.getRight());
    }

    /**
     * Request commit to the SQL service
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> commit() throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("already closed");
	}
	var rv = sessionLinkImpl.send(RequestProtos.Commit.newBuilder()
				.setTransactionHandle(transaction));
	cleanuped = true;
	close();
	return rv;
    }

    /**
     * Request rollback to the SQL service
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> rollback() throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("already closed");
	}
	var rv = sessionLinkImpl.send(RequestProtos.Rollback.newBuilder()
				  .setTransactionHandle(transaction));
	cleanuped = true;
	close();
	return rv;
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * This is used when the transaction is to be closed without commit or rollback.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
    }

    /**
     * Close the Transaction
     */
    public void close() throws IOException {
	if (Objects.nonNull(sessionLinkImpl)) {
	    if (!cleanuped) {
		try {
		    var futureResponse = sessionLinkImpl.send(RequestProtos.Rollback.newBuilder()
							      .setTransactionHandle(transaction));  // FIXME need to consider rollback is suitable here
		    var response = (timeout == 0) ? futureResponse.get() : futureResponse.get(timeout, unit);
		    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
			throw new IOException(response.getError().getDetail());
		    }
		} catch (TimeoutException | InterruptedException | ExecutionException e) {
		    throw new IOException(e);
		}
	    }
	    sessionLinkImpl.remove(this);
	    sessionLinkImpl = null;
	}
    }
}
