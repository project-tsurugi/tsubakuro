package com.nautilus_technologies.tsubakuro.impl.low.common;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.impl.low.sql.PreparedStatementImpl;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.impl.low.sql.FutureTransactionImpl;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * SessionImpl type.
 */
public class SessionImpl implements Session {
    private long timeout;
    private TimeUnit unit;
    private SessionLinkImpl sessionLinkImpl;
    
    /**
     * Connect this session to the SQL server.
     *
     * Note. How to connect to a SQL server is implementation dependent.
     * This implementation assumes that the session wire connected to the database is given.
     *
     * @param sessionWire the wire that connects to the Database
     */
    public void connect(SessionWire sessionWire) {
	this.sessionLinkImpl = new SessionLinkImpl(sessionWire);
    }

    /**
     * Begin a new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
    @Deprecated
    public Future<Transaction> createTransaction(boolean readOnly) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return new FutureTransactionImpl(sessionLinkImpl.send(
							      RequestProtos.Begin.newBuilder()
							      .setOption(RequestProtos.TransactionOption.newBuilder()
									 .setType(readOnly
										  ? RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_READ_ONLY
										  : RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_SHORT
										  )
									 )
							      )
					 , sessionLinkImpl);
    }

    /**
     * Begin a new read-write transaction
     * @return the transaction
     */
    public Future<Transaction> createTransaction() throws IOException {
	return createTransaction(RequestProtos.TransactionOption.newBuilder().build());
    }

    /**
     * Begin a new transaction by specifying the transaction type
     * @return the transaction
     */
    @Deprecated
    public Future<Transaction> createTransaction(RequestProtos.TransactionOption.Builder option) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return new FutureTransactionImpl(sessionLinkImpl.send(RequestProtos.Begin.newBuilder().setOption(option)), sessionLinkImpl);
    }
    public Future<Transaction> createTransaction(RequestProtos.TransactionOption option) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return new FutureTransactionImpl(sessionLinkImpl.send(RequestProtos.Begin.newBuilder().setOption(option)), sessionLinkImpl);
    }

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @param placeHolder the set of place holder name and type of its variable encoded with protocol buffer
     * @return a Future holding the result of the SQL service
     * @throws IOException error occurred in PREPARE
     */
    public Future<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder placeHolder) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return sessionLinkImpl.send(RequestProtos.Prepare.newBuilder()
				.setSql(sql)
				.setHostVariables(placeHolder));
    }
    @Deprecated
    public Future<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder.Builder placeHolder) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return sessionLinkImpl.send(RequestProtos.Prepare.newBuilder()
				.setSql(sql)
				.setHostVariables(placeHolder));
    }

    /**
     * Request explain to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a Future holding a string to explain the plan
     * @throws IOException error occurred in EXPLAIN
     */
    public Future<String> explain(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return sessionLinkImpl.send(RequestProtos.Explain.newBuilder()
				    .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
				    .setParameters(parameterSet));
    }
    @Deprecated
    public Future<String> explain(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
	if (Objects.isNull(sessionLinkImpl)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return sessionLinkImpl.send(RequestProtos.Explain.newBuilder()
				    .setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
				    .setParameters(parameterSet));
    }

    /**
     * Begin a new backup session (like transaction) by specifying the transaction type
     * @return the backup session
     */
    public Future<Backup> beginBackup() throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("this session is not connected to the Database");
        }
	return sessionLinkImpl.send();
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
    public void setCloseTimeout(long t, TimeUnit u) {
	timeout = t;
	unit = u;
    }
    
    /**
     * Close the Session
     */
    public void close() throws IOException {
	if (Objects.nonNull(sessionLinkImpl)) {
	    try {
		sessionLinkImpl.discardRemainingResources(timeout, unit);

		var futureResponse = sessionLinkImpl.send(RequestProtos.Disconnect.newBuilder());
		var response = (timeout == 0) ? futureResponse.get() : futureResponse.get(timeout, unit);
		if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
		    throw new IOException(response.getError().getDetail());
		}
	    } catch (TimeoutException | InterruptedException | ExecutionException e) {
		throw new IOException(e);
	    } finally {
		sessionLinkImpl.close();
		sessionLinkImpl = null;
	    }
	}
    }
}