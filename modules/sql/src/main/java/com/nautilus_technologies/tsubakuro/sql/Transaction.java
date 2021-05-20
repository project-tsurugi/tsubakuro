package com.nautilus_technologies.tsubakuro.sql;

import java.util.concurrent.Future;

/**
 * Transaction type.
 */
public interface Transaction {
    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    Future<ResponseProtos.ResultOnly> executeStatement(String sql);

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return Future<ResultSet> processing result of the SQL service
     */
    Future<ResultSet> executeQuery(String sql);

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    Future<ResponseProtos.ResultOnly> executeStatement(CommonProtos.PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet);

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResultSet> processing result of the SQL service
     */
    Future<ResultSet> executeQuery(CommonProtos.PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet);
}
