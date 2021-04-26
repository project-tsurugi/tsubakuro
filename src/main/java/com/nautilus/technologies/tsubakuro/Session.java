package com.nautilus.technologies.tsubakuro;

import com.nautilus.technologies.tsubakuro.Transaction;
import com.nautilus.technologies.tsubakuro.PreparedStatement;

/**
 * Provides session
 */
public class Session {
    /**
     * Creates a new instance.
     * @param connectionInfo information to connect to the database
     */
    public Session(String connectionInfo) {
    }

    /**
     * Begin the new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
    public Transaction createTransaction(boolean readOnly) {
	return new Transaction(readOnly);
    }

    /**
     * Begin the new transaction
     * @return the transaction
     */
    public Transaction createTransaction() {
	return createTransaction(false);
    }

    /**
     * Begin prepare sql statement and create prepared statement
     * @param sql the sql text string to prepare
     * @return the prepared statement
     */
    public PreparedStatement prepare(String sql) {
	return new PreparedStatement("");
    }
}
