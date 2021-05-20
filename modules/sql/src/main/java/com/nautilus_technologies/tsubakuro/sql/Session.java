package com.nautilus_technologies.tsubakuro.sql;

import java.util.concurrent.Future;

/**
 * Session type.
 */
public interface Session {
    /**
     * Begin the new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
    Future<Transaction> createTransaction(boolean readOnly);

    /**
     * Begin the new transaction
     * @return the transaction
     */
    Future<Transaction> createTransaction();

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @param praceHolder the set of place holder name and type of its variable encoded with protocol buffer
     * @return Future<PreparedStatement> holds the result of the SQL service
     */
    Future<ResponseProtos.Prepare> prepare(String sql, RequestProtos.PlaceHolder praceHolder);
}
