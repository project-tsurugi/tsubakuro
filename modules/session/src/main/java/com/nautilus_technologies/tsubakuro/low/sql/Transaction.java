package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * Transaction type.
 */
public interface Transaction extends ServerResource {

    /**
     * Executes a SQL statement.
     * @param source the SQL statement text
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResponseProtos.ResultOnly> executeStatement(@Nonnull String source) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a SQL statement.
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResponseProtos.ResultOnly> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull RequestProtos.ParameterSet.Parameter... parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        return executeStatement(statement, Arrays.asList(parameters));
    }

    /**
     * Executes a SQL statement.
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResponseProtos.ResultOnly> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a SQL statement and retrieve its result.
     * @param source the SQL statement text
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResultSet> executeQuery(@Nonnull String source) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a SQL statement and retrieve its result.
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResultSet> executeQuery(
            @Nonnull PreparedStatement statement,
            @Nonnull RequestProtos.ParameterSet.Parameter... parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        return executeQuery(statement, Arrays.asList(parameters));
    }

    /**
     * Executes a SQL statement and retrieve its result.
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResultSet> executeQuery(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a dump action.
     * <p>
     * This operation just executes a query, but will write the resulting relation data into dump files
     * onto the specified directory.
     * This will provide a result set that only contains the created dump file path,
     * in the first column of the relation.
     * </p>
     * @param source the SQL statement text, which must be a query like (e.g. {@code SELECT} statement)
     * @param directory the destination directory, which SQL service can create dump files to it
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResultSet> executeDump(
            @Nonnull String source,
            @Nonnull Path directory) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a dump action.
     * <p>
     * This operation just executes a query, but will write the resulting relation data into dump files
     * onto the specified directory.
     * This will provide a result set that only contains the created dump file path,
     * in the first column of the relation.
     * </p>
     * @param statement the prepared statement to execute, which must be a query like (e.g. {@code SELECT} statement)
     * @param parameters parameter list for place-holders in the prepared statement
     * @param directory the destination directory, which SQL service can create dump files onto it
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResultSet> executeDump(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters,
            @Nonnull Path directory) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a load action.
     * <p>
     * This operation performs like as following:
     * <ol>
     *   <li> extracts each row from the input dump files </li>
     *   <li> perform the statement for every rows, with substituting place-holders with the row data </li>
     * </ol>
     * </p>
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement.
     *      parameter can refer the column on the input dump file, by specifying
     *      {@link Parameters#referenceColumn(String, int)} or {@link Parameters#referenceColumn(String, String)}.
     *      It will substitutes place-holders into values from input dump file on runtime.
     * @param files the input dump file paths, which SQL server can read them
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResponseProtos.ResultOnly> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters,
            @Nonnull Path... files) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(files);
        return executeLoad(statement, parameters, Arrays.asList(files));
    }

    /**
     * Executes a load action.
     * <p>
     * This operation performs like as following:
     * <ol>
     *   <li> extracts each row from the input dump files </li>
     *   <li> perform the statement for every rows, with substituting place-holders with the row data </li>
     * </ol>
     * </p>
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement.
     *      parameter can refer the column on the input dump file, by specifying
     *      {@link Parameters#referenceColumn(String, int)} or {@link Parameters#referenceColumn(String, String)}.
     *      It will substitutes place-holders into values from input dump file on runtime.
     * @param files the input dump file paths, which SQL server can read them
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     * @see Parameters
     */
    default FutureResponse<ResponseProtos.ResultOnly> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters,
            @Nonnull Collection<? extends Path> files) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Request commit to the SQL service
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in commit by the SQL service
     */
    FutureResponse<ResponseProtos.ResultOnly> commit() throws IOException;

    /**
     * Request rollback to the SQL service
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in rollback by the SQL service
     */
    FutureResponse<ResponseProtos.ResultOnly> rollback() throws IOException;

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute statement by the SQL service
     * @deprecated use {@link #executeStatement(PreparedStatement, Collection)} instead
     */
    @Deprecated
    default FutureResponse<ResponseProtos.ResultOnly> executeStatement(
            PreparedStatement preparedStatement,
            RequestProtos.ParameterSet parameterSet) throws IOException {
        return executeStatement(preparedStatement, parameterSet.getParametersList());
    }

    /**
     * Request executeStatement to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute statement by the SQL service
     * @deprecated use {@link #executeStatement(PreparedStatement, Collection)} instead
     */
    @Deprecated
    default FutureResponse<ResponseProtos.ResultOnly> executeStatement(
            PreparedStatement preparedStatement,
            RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
        return executeStatement(preparedStatement, parameterSet.getParametersList());
    }

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a FutureResponse of ResultSet which is a processing result of the SQL service
     * @throws IOException error occurred in execute query by the SQL service
     * @deprecated use {@link #executeQuery(PreparedStatement, Collection)} instead
     */
    @Deprecated
    default FutureResponse<ResultSet> executeQuery(
            PreparedStatement preparedStatement,
            RequestProtos.ParameterSet parameterSet) throws IOException {
        return executeQuery(preparedStatement, parameterSet.getParametersList());
    }

    /**
     * Request executeQuery to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a FutureResponse of ResultSet which is a processing result of the SQL service
     * @throws IOException error occurred in execute query by the SQL service
     * @deprecated use {@link #executeQuery(PreparedStatement, Collection)} instead
     */
    @Deprecated
    default FutureResponse<ResultSet> executeQuery(
            PreparedStatement preparedStatement,
            RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
        return executeQuery(preparedStatement, parameterSet.getParametersList());
    }

    /**
     * Request dump execution to the SQL service
     * @param preparedStatement prepared statement used in the dump operation
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @param directory the directory path where dumped files are placed
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute dump by the SQL service
     * @deprecated use {@link #executeDump(PreparedStatement, Collection, Path)} instead
     */
    @Deprecated
    default FutureResponse<ResultSet> executeDump(
            PreparedStatement preparedStatement,
            RequestProtos.ParameterSet parameterSet,
            Path directory) throws IOException {
        return executeDump(preparedStatement, parameterSet.getParametersList(), directory);
    }

    /**
     * Request load execution to the SQL service
     * @param preparedStatement prepared statement used in the dump operation
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @param files the collection of file path to be loaded
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in execute load by the SQL service
     * @deprecated use {@link #executeLoad(PreparedStatement, Collection, Collection)} instead
     */
    @Deprecated
    default FutureResponse<ResponseProtos.ResultOnly> executeLoad(
            PreparedStatement preparedStatement,
            RequestProtos.ParameterSet parameterSet,
            Collection<? extends Path> files) throws IOException {
        return executeLoad(preparedStatement, parameterSet.getParametersList(), files);
    }
}
