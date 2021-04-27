package com.nautilus.technologies.tsubakuro;

import com.nautilus.technologies.tsubakuro.Transaction;
import com.nautilus.technologies.tsubakuro.PreparedStatement;

/**
 * Session type.
 */
public interface Session {
    /**
     * Begin the new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
    public Transaction createTransaction(boolean readOnly);

    /**
     * Begin the new transaction
     * @return the transaction
     */
    public Transaction createTransaction();

    /**
     * Begin prepare sql statement and create prepared statement
     * @param sql the sql text string to prepare
     * @return the prepared statement
     */
    public PreparedStatement prepare(String sql);
}
