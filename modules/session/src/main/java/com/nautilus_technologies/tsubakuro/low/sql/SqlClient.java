package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SqlClientImpl;
import  com.nautilus_technologies.tsubakuro.low.common.Session;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * A SQL service client.
 * @see #attach(Session)
 */
public interface SqlClient extends ServerResource {

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
     * @see #createTransaction(com.tsurugidb.jogasaki.proto.SqlRequest.TransactionOption)
     */
    default FutureResponse<Transaction> createTransaction() throws IOException {
        return createTransaction(SqlRequest.TransactionOption.getDefaultInstance());
    }

    /**
     * Starts a new transaction.
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
     * @param source the SQL statement text
     * @return a future response of statement metadata
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<String> explain(@Nonnull String source) throws IOException {
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
    default FutureResponse<String> explain(
            @Nonnull PreparedStatement statement,
            @Nonnull SqlRequest.Parameter... parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        return explain(statement, Arrays.asList(parameters));
    }

    /**
     * Retrieves execution plan of the statement.
     * @param statement the prepared statement
     * @param parameters parameter list for place-holders in the prepared statement
     * @return a future response of statement metadata
     * @throws IOException if I/O error was occurred while sending request
     * @see Parameters
     */
    default FutureResponse<String> explain(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves metadata for a table.
     * @param tableName the target table name
     * @return a future response of table metadata
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<TableMetadata> getTableMetadata(@Nonnull String tableName) throws IOException {
        Objects.requireNonNull(tableName);
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
