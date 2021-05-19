package com.nautilus_technologies.tsubakuro;

import java.util.concurrent.Future;

/**
 * Transaction type.
 */
public interface Transaction {
    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return Future<ErrorCode> indicate whether the command is processed successfully or not
     */
    Future<ErrorCode> executeStatement(String sql);

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return Future<ResultSet> processing result of the SQL service
     */
    Future<ResultSet> executeQuery(String sql);

    /**
     * Request executeStatement to the SQL service
     * @param request the set of prepared statement and parameter set for the command
     * @return Future<ErrorCode> indicate whether the command is processed successfully or not
     */
    Future<ErrorCode> executeStatement(ExecutablePreparedStatementRequest request);

    /**
     * Request executeQuery to the SQL service
     * @param request the set of prepared statement and parameter set for the command
     * @return Future<ResultSet> processing result of the SQL service
     */
    Future<ResultSet> executeQuery(ExecutablePreparedStatementRequest request);
}
