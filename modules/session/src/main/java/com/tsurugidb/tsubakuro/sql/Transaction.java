/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResourceNeedingDisposal;

/**
 * Transaction type.
 */
public interface Transaction extends ServerResourceNeedingDisposal {

    /**
     * Executes a SQL statement.
     * If the return value, describing future response of the action, is not gotten before the transaction close,
     * the outcome of the invocation will be indefinite.
     * @param source the SQL statement text
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ExecuteResult> executeStatement(@Nonnull String source) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a SQL statement.
     * If the return value, describing future response of the action, is not gotten before the transaction close,
     * the outcome of the invocation will be indefinite.
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ExecuteResult> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull SqlRequest.Parameter... parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        return executeStatement(statement, Arrays.asList(parameters));
    }

    /**
     * Executes a SQL statement.
     * If the return value, describing future response of the action, is not gotten before the transaction close,
     * the outcome of the invocation will be indefinite.
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ExecuteResult> executeStatement(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a SQL statement and retrieve its result.
     * No valid data can be obtained from a ResultSet that is gotton after the transaction close.
     * @param source the SQL statement text
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ResultSet> executeQuery(@Nonnull String source) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a SQL statement and retrieve its result.
     * No valid data can be obtained from a ResultSet that is gotton after the transaction close.
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
     * No valid data can be obtained from a ResultSet that is gotton after the transaction close.
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
     * <em>This method is not yet implemented:</em>
     * Executes a SQL statement with 2-dimension parameter table.
     * <p>
     * This operation may raise {@link IOException} if the 2-D parameter table is too large to send a request.
     * Please split the parameter table and execute {@link #batch(PreparedStatement, Collection) batch()} for the
     * individual fragment of the parameter table.
     * For large parameter tables, please consider to use
     * {@link #executeLoad(PreparedStatement, Collection, Collection) load()} instead.
     * </p>
     * @param statement the prepared statement to execute for each 1-D parameter set
     * @param parameterTable 2-D parameter table (list of 1-D parameter set) for place-holders in the prepared statement
     * @return a future response of the action
     * @throws IOException if I/O error was occurred while sending request, or the parameter table is too large
     */
    default FutureResponse<ExecuteResult> batch(
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
     * </p>
     * <ol>
     *   <li> extracts each row from the input dump files </li>
     *   <li> perform the statement for every rows, with substituting place-holders with the row data </li>
     * </ol>
     * @param statement the prepared statement to execute
     * @param parameters parameter list for place-holders in the prepared statement.
     *      parameter can refer the column on the input dump file, by specifying
     *      {@link Parameters#referenceColumn(String, int)} or {@link Parameters#referenceColumn(String, String)}.
     *      It will substitutes place-holders into values from input dump file on runtime.
     * @param files the input dump file paths, which SQL server can read them
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<ExecuteResult> executeLoad(
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
     * </p>
     * <ol>
     *   <li> extracts each row from the input dump files </li>
     *   <li> perform the statement for every rows, with substituting place-holders with the row data </li>
     * </ol>
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
    default FutureResponse<ExecuteResult> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Collection<? extends Path> files) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the transaction status on the server.
     * @return the transaction status on the server side, which may have changed by the time this result is received
     * @throws IOException if I/O error was occurred while sending request
     * @see TransactionStatus
     */
    default FutureResponse<TransactionStatus> getStatus() throws IOException {
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

    /**
    * Returns occurred error in the target transaction, only if the transaction has been accidentally aborted.
    * @return the future response of the error information:
    * if the transaction will have been accidentally aborted, this provides the occurred error information.
    * otherwise, the transaction is running, successfully committed, or manually aborted, then this will provide {@code null}
    * The returned object may <b>raise exception</b> if this operation occurs a new error in the server side.
    * @throws IOException if I/O error was occurred while sending request
    */
    default FutureResponse<SqlServiceException> getSqlServiceException() throws IOException {
        throw new UnsupportedOperationException();
    }


    /**
     * Returns an input stream for the blob.
     * @param ref the blob reference
     * @return a future response of an input stream for the blob
     * @throws IOException if I/O error was occurred while sending request
     *
     * @since 1.8.0
     */
    default FutureResponse<InputStream> openInputStream(BlobReference ref) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a reader for the clob.
     * @param ref the clob reference
     * @return a future response of a reader for the clob
     * @throws IOException if I/O error was occurred while sending request
     *
     * @since 1.8.0
     */
    default FutureResponse<Reader> openReader(ClobReference ref) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns an object cache for the clob.
     * @param ref the large object reference
     * @return a future response of a LargeObjectCache
     * @throws IOException if I/O error was occurred while sending request
     *
     * @since 1.8.0
     */
    default FutureResponse<LargeObjectCache> getLargeObjectCache(LargeObjectReference ref) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Copy the large object to the file indicated by the given path.
     * @param ref the large object reference
     * @param destination the path of the destination file
     * @return a future response of Void, whose get() or await() may throws a BlobException meaning an error was occurred while copying the BLOB data
     * @throws IOException if I/O error was occurred while sending request
     *
     * @since 1.8.0
     */
    default FutureResponse<Void> copyTo(LargeObjectReference ref, Path destination) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Provides transaction id that is unique to for the duration of the database server's lifetime
     * @return the id String for this transaction
     */
    default String getTransactionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        return;
    }
}
