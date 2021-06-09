package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.io.IOException;
import java.io.Closeable;

/**
 * Session type.
 */
public interface Session extends Closeable {
    /**
     * Begin the new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
    Future<Transaction> createTransaction(boolean readOnly) throws IOException;

    /**
     * Begin the new transaction
     * @return the transaction
     */
    Future<Transaction> createTransaction() throws IOException;

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @param placeHolder the set of place holder name and type of its variable encoded with protocol buffer
     * @return Future<PreparedStatement> holds the result of the SQL service
     */
    Future<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder placeHolder) throws IOException;
}
