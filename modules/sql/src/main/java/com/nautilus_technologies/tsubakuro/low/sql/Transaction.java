package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * Transaction type.
 */
public interface Transaction extends Closeable {
    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    Future<ResponseProtos.ResultOnly> executeStatement(String sql) throws IOException;

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return Future<ResultSet> processing result of the SQL service
     */
    Future<ResultSet> executeQuery(String sql) throws IOException;

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    Future<ResponseProtos.ResultOnly> executeStatement(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException;

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return Future<ResultSet> processing result of the SQL service
     */
    Future<ResultSet> executeQuery(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException;

    /**
     * Request commit to the SQL service
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    Future<ResponseProtos.ResultOnly> commit() throws IOException;

    /**
     * Request rollback to the SQL service
     * @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
     */
    Future<ResponseProtos.ResultOnly> rollback() throws IOException;
}
