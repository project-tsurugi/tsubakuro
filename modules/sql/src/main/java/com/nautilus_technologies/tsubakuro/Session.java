package com.nautilus_technologies.tsubakuro;

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
    Transaction createTransaction(boolean readOnly);

    /**
     * Begin the new transaction
     * @return the transaction
     */
    Transaction createTransaction();

    /**
     * Request prepare to the SQL service
     * @param prepareRequest the PrepareRequest class consisging of sql and the set of place holder definition
     * @return Future<PreparedStatement> holds the result of the SQL service
     */
    Future<PreparedStatement> prepare(PrepareRequest prepareRequest);
}
