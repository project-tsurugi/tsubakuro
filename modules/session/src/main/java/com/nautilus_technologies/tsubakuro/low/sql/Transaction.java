package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tateyama.proto.SqlRequest;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
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
    default FutureResponse<Void> executeStatement(@Nonnull String source) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a SQL statement.
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull SqlRequest.Parameter... parameters) throws IOException {
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
    default FutureResponse<Void> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
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
            @Nonnull SqlRequest.Parameter... parameters) throws IOException {
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
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
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
     * Executes a SQL statement with 2-dimension parameter table.
     * <p>
     * This operation may raise {@link IOException} if the 2-D parameter table is too large to send a request.
     * Please split the parameter table and execute {@link #batch(PreparedStatement, Collection) batch()} for the
     * individual fragment of the parameter table.
     * For large parameter tables, please consider to use
     * {@link #load(PreparedStatement, Collection, Collection) load()} instead.
     * </p>
     * @param statement the prepared statement to execute for each 1-D parameter set
     * @param parameterTable 2-D parameter table (list of 1-D parameter set) for place-holders in the prepared statement
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request, or the parameter table is too large
     */
    default FutureResponse<Void> batch(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends Collection<? extends SqlRequest.Parameter>> parameterTable)
                    throws IOException {
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
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Path directory) throws IOException {
        return executeDump(statement, parameters, directory, SqlRequest.DumpOption.getDefaultInstance());
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
     * @param option the options to customize dump execution
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResultSet> executeDump(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Path directory,
            @Nonnull SqlRequest.DumpOption option) throws IOException {
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
    default FutureResponse<Void> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
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
    default FutureResponse<Void> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Collection<? extends Path> files) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Request commit to the SQL service
     * @return a FutureResponse of SqlResponse.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in commit by the SQL service
     */
    default FutureResponse<Void> commit() throws IOException {
        return commit(SqlRequest.CommitStatus.COMMIT_STATUS_UNSPECIFIED);
    }

    /**
     * Commits the current transaction.
     * @param status the commit status which the request is waiting for
     * @return a future response of this action:
     *      the response will be returned after the transaction will reach the commit status,
     *      or raise error if the commit operation was failed
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> commit(@Nonnull SqlRequest.CommitStatus status) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Request rollback to the SQL service
     * @return a FutureResponse of SqlResponse.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in rollback by the SQL service
     */
    default FutureResponse<Void> rollback() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        return;
    }
}
