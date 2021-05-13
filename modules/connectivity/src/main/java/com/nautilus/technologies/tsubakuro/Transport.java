package com.nautilus.technologies.tsubakuro;

import java.util.concurrent.Future;
import com.nautilus.technologies.tsubakuro.PreparedStatement;
import com.nautilus.technologies.tsubakuro.ParameterSet;
import com.nautilus.technologies.tsubakuro.ResultSet;

/**
 * Transport type.
 */
public interface Transport {
    /**
     * Describes error code
     */
    public enum ErrorCode {
        /**
         * @brief no error
         */
        OK,

        /**
         * @brief some error
         * @note Detailed error codes will be determined in the future
         */
        SOME_ERROR;
    }

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @return Future<PreparedStatement> processing result of the SQL service, which will be given to executeQuery and/or executeStatement
     */
    public Future<PreparedStatement> prepare(String sql);

    /**
     * Request executeStatement to the SQL service
     * @param sql sql text for the command
     * @return Future<ErrorCode> indicate whether the command is processed successfully or not
     */
    public Future<ErrorCode> executeStatement(String sql);

    /**
     * Request executeQuery to the SQL service
     * @param sql sql text for the command
     * @return Future<ResultSet> processing result of the SQL service
     */
    public Future<ResultSet> executeQuery(String sql);

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement
     * @return Future<ErrorCode> indicate whether the command is processed successfully or not
     */
    public Future<ErrorCode> executeStatement(PreparedStatement preparedStatement, ParameterSet parameterSet);

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement
     * @return Future<ResultSet> processing result of the SQL service
     */
    public Future<ResultSet> executeQuery(PreparedStatement preparedStatement, ParameterSet parameterSet);
}
