package com.nautilus.technologies.tsubakuro;

import com.nautilus.technologies.tsubakuro.PreparedStatement;
import com.nautilus.technologies.tsubakuro.ParameterSet;
import com.nautilus.technologies.tsubakuro.ResultSet;

/**
 * Provides transaction
 */
public class Transaction {
    /**
     * Creates a new instance.
     * @param readOnly specify whether the new transaction is read-only or not
     */
    Transaction(boolean readOnly) {
    }

    /**
     * Execute the query in the transaction
     * @param statement prepared statement
     * @param parameters the parameters to assign value for each placeholder
     * @return the result
     */
    public ResultSet executeQuery(PreparedStatement statement, ParameterSet parameters) {
	return new ResultSet();
    }

    /**
     * Execute the query in the transaction
     * @param sql the sql text string
     * @return the result
     */
    public ResultSet executeQuery(String sql) {
	return new ResultSet();
    }

    /**
     * Execute the statement in the transaction
     * @param statement prepared statement
     * @param parameters the parameters to assign value for each placeholder
     */

    public void executeStatement(PreparedStatement statement, ParameterSet parameters) {
    }
    /**
     * Execute the statement in the transaction
     * @param sql the sql text string
     */
    public void executeStatement(String sql) {
    }
}
