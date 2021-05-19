package com.nautilus_technologies.tsubakuro;

/**
 * PreparedStatement type.
 */
public interface PreparedStatement {
    /**
     * Create a ExecutablePreparedStatementRequest class to be used in execute prepared statement or query request
     * @return new ExecutablePreparedStatement
     */
    ExecutablePreparedStatementRequest create();
}
