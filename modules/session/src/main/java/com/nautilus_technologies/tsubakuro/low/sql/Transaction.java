package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.util.Collection;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * Transaction type.
 */
public interface Transaction extends Closeable {
    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute statement by the SQL service
     */
    Future<ResponseProtos.ResultOnly> executeStatement(String sql) throws IOException;

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return a Future of ResultSet which is a processing result of the SQL service
     * @throws IOException error occurred in execute query by the SQL service
    */
    Future<ResultSet> executeQuery(String sql) throws IOException;

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute statement by the SQL service
     */
    Future<ResponseProtos.ResultOnly> executeStatement(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException;
    @Deprecated
    Future<ResponseProtos.ResultOnly> executeStatement(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException;

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a Future of ResultSet which is a processing result of the SQL service
     * @throws IOException error occurred in execute query by the SQL service
     */
    Future<ResultSet> executeQuery(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException;
    @Deprecated
    Future<ResultSet> executeQuery(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException;

    /**
     * Request dump execution to the SQL service
     * @param preparedStatement prepared statement used in the dump operation
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @param directory the directory path where dumped files are placed
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute dump by the SQL service
     */
    Future<ResultSet> executeDump(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet, Path directory) throws IOException;

    /**
     * Request load execution to the SQL service
     * @param preparedStatement prepared statement used in the dump operation
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @param files the collection of file path to be loaded
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute load by the SQL service
     */
    Future<ResponseProtos.ResultOnly> executeLoad(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet,	Collection<? extends Path> files) throws IOException;

    /**
     * Request commit to the SQL service
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in commit by the SQL service
     */
    Future<ResponseProtos.ResultOnly> commit() throws IOException;

    /**
     * Request rollback to the SQL service
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in rollback by the SQL service
     */
    Future<ResponseProtos.ResultOnly> rollback() throws IOException;
}
