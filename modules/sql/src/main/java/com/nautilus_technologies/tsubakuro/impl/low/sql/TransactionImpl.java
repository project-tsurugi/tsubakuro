package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * Transaction type.
 */
public class TransactionImpl implements Transaction {
    SessionWire sessionWire;
    CommonProtos.Transaction transaction;
    boolean done;
    
    public TransactionImpl(SessionWire w, CommonProtos.Transaction t) {
	sessionWire = w;
	transaction = t;
	done = false;
    }

    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> executeStatement(String sql) throws IOException {
	return sessionWire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder()
						    .setExecuteStatement(RequestProtos.ExecuteStatement.newBuilder()
									 .setTransactionHandle(transaction)
									 .setSql(sql)
									 ).build(),
						    new FutureResponse.ResultOnlyDistiller());
    }

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return Future<ResultSet> processing result of the SQL service
     */
    public Future<ResultSet> executeQuery(String sql) throws IOException {
	return new FutureResultSetImpl(sessionWire.<ResponseProtos.ExecuteQuery>send(RequestProtos.Request.newBuilder()
										     .setExecuteQuery(RequestProtos.ExecuteQuery.newBuilder()
												      .setTransactionHandle(transaction)
												      .setSql(sql))
										     .build(),
										     new FutureResponse.ExecuteQueryDistiller()), sessionWire
				       );
    };

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    public Future<ResponseProtos.ResultOnly> executeStatement(CommonProtos.PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException {
    	return sessionWire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder()
						    .setExecutePreparedStatement(RequestProtos.ExecutePreparedStatement.newBuilder()
										 .setTransactionHandle(transaction)
										 .setPreparedStatementHandle(preparedStatement)
										 .setParameters(parameterSet))
						    .build(),
						    new FutureResponse.ResultOnlyDistiller());
    }

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResultSet> processing result of the SQL service
     */
    public Future<ResultSet> executeQuery(CommonProtos.PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException {
	return new FutureResultSetImpl(sessionWire.<ResponseProtos.ExecuteQuery>send(RequestProtos.Request.newBuilder()
										     .setExecutePreparedStatement(RequestProtos.ExecutePreparedStatement.newBuilder()
														  .setTransactionHandle(transaction)
														  .setPreparedStatementHandle(preparedStatement)
														  .setParameters(parameterSet))
										     .build(),
										     new FutureResponse.ExecuteQueryDistiller()), sessionWire
				       );
    }

    /**
     * Close the Transaction
     */
    public void close() throws IOException {
        if (!done) {
	    throw new IOException("error: TransactionImpl.close()");
	}
    }
}
