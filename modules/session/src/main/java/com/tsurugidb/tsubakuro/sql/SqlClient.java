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
import com.tsurugidb.tsubakuro.client.ServiceClient;
import com.tsurugidb.tsubakuro.client.ServiceMessageVersion;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.SqlClientImpl;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A SQL service client.
 * @see #attach(Session)
 */
@ServiceMessageVersion(
        service = SqlClient.SERVICE_SYMBOLIC_ID,
        major = SqlClient.SERVICE_MESSAGE_VERSION_MAJOR,
        minor = SqlClient.SERVICE_MESSAGE_VERSION_MINOR)
public interface SqlClient extends ServerResource, ServiceClient {

    /**
     * The symbolic ID of the destination service.
    */
    String SERVICE_SYMBOLIC_ID = "sql";

    /**
     * The major service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MAJOR = 1;

    /**
     * The minor service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MINOR = 4;

    /**
     * Attaches to the SQL service in the current session.
     * @param session the current session
     * @return the SQL service client
     */
    static SqlClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return SqlClientImpl.attach(session);
    }

    /**
     * Starts a new transaction with default transaction options.
     * @return a future response of transaction object
     * @throws IOException if I/O error was occurred while sending request
     * @see #createTransaction(com.tsurugidb.sql.proto.SqlRequest.TransactionOption)
     */
    default FutureResponse<Transaction> createTransaction() throws IOException {
        return createTransaction(SqlRequest.TransactionOption.getDefaultInstance());
    }

    /**
     * Starts a new transaction.
     * The consequence of close() call of the returned {@code FutureResponse<Transaction>} is undefined,
     * if the close() is called without get() the Transaction object.
     * @param option the transaction option
     * @return a future response of transaction object
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Transaction> createTransaction(
            @Nonnull SqlRequest.TransactionOption option) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Prepares a SQL statement.
     * @param source the SQL statement text (may includes place-holders)
     * @param placeholders the place-holders in the statement text
     * @return a future response of prepared statement object
     * @throws IOException if I/O error was occurred while sending request
     * @see #prepare(String, Collection)
     * @see Placeholders
     */
    default FutureResponse<PreparedStatement> prepare(
            @Nonnull String source,
            @Nonnull SqlRequest.Placeholder... placeholders) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(placeholders);
        return prepare(source, Arrays.asList(placeholders));
    }

    /**
     * Prepares a SQL statement.
     * The consequence of close() call of the returned {@code FutureResponse<PreparedStatement>} is undefined,
     * if the close() is called without get() the PreparedStatement object.
     * @param source the SQL statement text (may includes place-holders)
     * @param placeholders the place-holders in the statement text
     * @return a future response of prepared statement object
     * @throws IOException if I/O error was occurred while sending request
     * @see Placeholders
     */
    default FutureResponse<PreparedStatement> prepare(
            @Nonnull String source,
            @Nonnull Collection<? extends SqlRequest.Placeholder> placeholders) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves execution plan of the statement.
     * The consequence of close() call of the returned {@code FutureResponse<StatementMetadata>} is undefined,
     * if the close() is called without get() the StatementMetadata object.
     * @param source the SQL statement text
     * @return a future response of statement metadata
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<StatementMetadata> explain(@Nonnull String source) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves execution plan of the statement.
     * @param statement the prepared statement
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of statement metadata
     * @throws IOException if I/O error was occurred while sending request
     * @see #explain(PreparedStatement, Collection)
     * @see Parameters
     */
    default FutureResponse<StatementMetadata> explain(
            @Nonnull PreparedStatement statement,
            @Nonnull SqlRequest.Parameter... parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        return explain(statement, Arrays.asList(parameters));
    }

    /**
     * Retrieves execution plan of the statement.
     * The consequence of close() call of the returned {@code FutureResponse<StatementMetadata>} is undefined,
     * if the close() is called without get() the StatementMetadata object.
     * @param statement the prepared statement
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of statement metadata
     * @throws IOException if I/O error was occurred while sending request
     * @see Parameters
     */
    default FutureResponse<StatementMetadata> explain(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves metadata for a table.
     * The consequence of close() call of the returned {@code FutureResponse<TableMetadata>} is undefined,
     * if the close() is called without get() the TableMetadata object.
     * @param tableName the target table name
     * @return a future response of table metadata
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<TableMetadata> getTableMetadata(@Nonnull String tableName) throws IOException {
        Objects.requireNonNull(tableName);
        throw new UnsupportedOperationException();
    }

    /**
     * Executes a load action out side transaction context.
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
     * Returns the list of available table names in the database, except system tables.
     * <p>
     * The table names are each fully qualified (maybe with a schema name).
     * To retrieve more details for the individual tables, you can use {@link #getTableMetadata(String)}.
     * </p>
     * @return a future response of available table names
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<TableList> listTables() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the current search path.
     * @return a future response of the current search path
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<SearchPath> getSearchPath() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns an input stream for the blob.
     * @return a future response of an input stream for the blob
     * @param ref the blob reference
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<InputStream> openInputStream(BlobReference ref) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a reader for the clob.
     * @return a future response of a reader for the clob
     * @param ref the clob reference
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Reader> openReader(ClobReference ref) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Disposes the underlying server resources.
     * Note that, this never closes the underlying {@link Session}.
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing in default.
    }
}
