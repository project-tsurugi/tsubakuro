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
     * @param sql sql text for the command
     * @return Future<PreparedStatement> processing result of the SQL service, which will be given to executeQuery and/or executeStatement
     */
    Future<PreparedStatement> prepare(String sql, ParameterSet parameterSet);
}
