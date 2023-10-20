package com.tsurugidb.tsubakuro.sql;

/**
 * Represents a kind of execution counter of SQL statements.
 */
public enum CounterType {

    /**
     * The number of rows inserted in the execution.
     */
    INSERTED_ROWS,

    /**
     * The number of rows updated in the execution.
     */
    UPDATED_ROWS,

    /**
     * The number of rows inserted or replaced/updated or inserted in the execution.
     */
    MERGED_ROWS,

    /**
     * The number of rows deleted in the execution.
     */
    DELETED_ROWS,
}
