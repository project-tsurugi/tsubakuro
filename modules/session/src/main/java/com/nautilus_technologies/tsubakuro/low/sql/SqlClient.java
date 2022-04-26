package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SqlClientImpl;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
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
        return new SqlClientImpl(session);
    }


    /**
     * Begins a new transaction.
     * @return a Future of the transaction
     * @throws IOException error occurred in BEGIN
     */
    default Future<Transaction> createTransaction() throws IOException {
        return createTransaction(RequestProtos.TransactionOption.getDefaultInstance());
    }

    /**
     * Begins a new transaction.
     * @param option the transaction options
     * @return a Future of the transaction
     * @throws IOException error occurred in BEGIN
     */
    default Future<Transaction> createTransaction(@Nonnull RequestProtos.TransactionOption option) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Prepares SQL statement text.
     * @param sql the SQL text
     * @return a Future holding the result of the SQL service
     * @throws IOException error occurred in PREPARE
     * @see #prepare(String, com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder)
     */
    default Future<PreparedStatement> prepare(@Nonnull String sql) throws IOException {
        Objects.requireNonNull(sql);
        return prepare(sql, RequestProtos.PlaceHolder.getDefaultInstance());
    }

    /**
     * Prepares SQL statement text.
     * @param sql the SQL text
     * @param placeHolder the set of place holder name and type of its variable
     * @return a Future holding the result of the SQL service
     * @throws IOException error occurred in PREPARE
     */
    default Future<PreparedStatement> prepare(@Nonnull String sql, @Nonnull RequestProtos.PlaceHolder placeHolder)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves an execution plan of the SQL statement.
     * @param statement the target statement
     * @return a Future holding a string to explain the plan
     * @throws IOException error occurred in EXPLAIN
     */
    default Future<String> explain(@Nonnull PreparedStatement statement) throws IOException {
        return explain(statement, RequestProtos.ParameterSet.getDefaultInstance());
    }

    /**
     * Retrieves an execution plan of the SQL statement.
     * @param statement the target statement
     * @param parameters parameter set for the statement
     * @return a Future holding a string to explain the plan
     * @throws IOException error occurred in EXPLAIN
     */
    default Future<String> explain(@Nonnull PreparedStatement statement, @Nonnull RequestProtos.ParameterSet parameters)
            throws IOException {
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
