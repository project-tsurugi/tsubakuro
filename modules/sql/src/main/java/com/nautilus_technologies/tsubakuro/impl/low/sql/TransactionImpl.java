package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
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
    SessionLinkImpl sessionLink;
    CommonProtos.Transaction transaction;
    
    public TransactionImpl(SessionLinkImpl sessionLink, CommonProtos.Transaction transaction) {
	this.sessionLink = sessionLink;
	this.transaction = transaction;
    }

    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> executeStatement(String sql) throws IOException {
	return sessionLink.send(RequestProtos.ExecuteStatement.newBuilder()
				.setTransactionHandle(transaction)
				.setSql(sql));
    }

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return Future<ResultSet> processing result of the SQL service
     */
    public Future<ResultSet> executeQuery(String sql) throws IOException {
	return new FutureResultSetImpl(sessionLink.send(RequestProtos.ExecuteQuery.newBuilder()
							.setTransactionHandle(transaction)
							.setSql(sql)),
				       sessionLink);
    };

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> executeStatement(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
    	return sessionLink.send(RequestProtos.ExecutePreparedStatement.newBuilder()
				.setTransactionHandle(transaction)
				.setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
				.setParameters(parameterSet));
    }

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResultSet> processing result of the SQL service
     */
    public Future<ResultSet> executeQuery(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
	
	return new FutureResultSetImpl(sessionLink.send(RequestProtos.ExecutePreparedQuery.newBuilder()
							.setTransactionHandle(transaction)
							.setPreparedStatementHandle(((PreparedStatementImpl) preparedStatement).getHandle())
							.setParameters(parameterSet)),
				       sessionLink);
    }

    /**
     * Request commit to the SQL service
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> commit() throws IOException {
	return sessionLink.send(RequestProtos.Commit.newBuilder()
				.setTransactionHandle(transaction));
    }

    /**
     * Request rollback to the SQL service
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> rollback() throws IOException {
	return sessionLink.send(RequestProtos.Rollback.newBuilder()
				.setTransactionHandle(transaction));
    }

    /**
     * Close the Transaction
     */
    public void close() throws IOException {  // FIXME
    }
}
