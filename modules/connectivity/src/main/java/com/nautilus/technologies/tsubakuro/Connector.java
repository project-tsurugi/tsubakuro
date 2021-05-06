package com.nautilus.technologies.tsubakuro;

import com.nautilus.technologies.tsubakuro.PreparedStatement;
import com.nautilus.technologies.tsubakuro.ParameterSet;
import com.nautilus.technologies.tsubakuro.ResultSet;

/**
 * Connector type.
 */
public interface Connector {
    /**
     * Describes request type
     */
    public enum RequestType {
        /**
         * @brief prepare
         * @note param SQL, return PreparedStatement
         */
        Prepare,

        /**
         * @brief execute statement
         * @note param SQL
         */
        ExecuteStatement,

        /**
         * @brief execute query
         * @note param SQL, return ResultSet
         */
        ExecuteQuery,

        /**
         * @brief execute prepared statement
         * @note param PreparedStatement and ParameterSet
         */
        ExecutePreparedStatement,

        /**
         * @brief execute prepqred query
         * @note param PreparedStatement and ParameterSet, return ResultSet
         */
        ExecutePreparedQuery,

	/**
         * @brief disconnect
         */
        Disconnect;
    }

    /**
     * Describes error code
     */
    public enum ErrorCode {
        /**
         * @brief no error
         */
        Ok,

        /**
         * @brief some error
         * @note Detailed error codes will be determined in the future
         */
        SomeError;
    }

    /**
     * Create Connector to the SQL service specified by the connectionInfo param.
     * @param connectionInfo informantion of connection to the SQL service
     */
    public void connect(String connectionInfo);

    /**
     * Send command and parameter to the SQL service, for Prepare, ExecuteStatement, and/or ExecuteQuery
     * @param command the type of the request
     * @param sql sql text for the command
     */
    public void sendRequest(RequestType command, String sql);

    /**
     * Send command and parameter to the SQL service, for ExecutePreparedStatement and/or ExecutePreparedQuery
     * @param command the type of the request
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement
     */
    public void sendRequest(RequestType command, PreparedStatement preparedStatement, ParameterSet parameterSet);

    /**
     * Send command to the SQL service, for Disconnect
     * @param command the type of the request
     */
    public void sendRequest(RequestType command);

    /**
     * Get ErrorCode of the command
     * @return ErrorCode indicate whether the command is processed successfully or not
     */
    public ErrorCode getErrorCode();

    /**
     * Get PreparedStatement returnded from the SQL service
     * @return ErrorCode indicate whether the command is processed successfully or not
     */
    public PreparedStatement getPreparedStatement();

    /**
     * Get resultSet returned from the SQL service
     * @return errorCode indicate whether the command is processed successfully or not
     */
    public ResultSet getResultSet();
}
