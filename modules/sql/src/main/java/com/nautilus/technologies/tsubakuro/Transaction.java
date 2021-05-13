package com.nautilus.technologies.tsubakuro;

import com.nautilus.technologies.tsubakuro.PreparedStatement;
import com.nautilus.technologies.tsubakuro.ParameterSet;
import com.nautilus.technologies.tsubakuro.ResultSet;

/**
 * Transaction type.
 */
public interface Transaction {
    /**
     * Execute the query in the transaction
     * @param statement prepared statement
     * @param parameters the parameters to assign value for each placeholder
     * @return the result
     */
    ResultSet executeQuery(PreparedStatement statement, ParameterSet parameters);

    /**
     * Execute the query in the transaction
     * @param sql the sql text string
     * @return the result
     */
    ResultSet executeQuery(String sql);

    /**
     * Execute the statement in the transaction
     * @param statement prepared statement
     * @param parameters the parameters to assign value for each placeholder
     */
    void executeStatement(PreparedStatement statement, ParameterSet parameters);

    /**
     * Execute the statement in the transaction
     * @param sql the sql text string
     */
    void executeStatement(String sql);
}
