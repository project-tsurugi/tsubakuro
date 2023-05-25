package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a result of {@code REMOVE} request.
 */
public interface RemoveResult {

    /**
     * Returns the number of records which the operation actually written.
     * @return the number of records
     */
    int size(); // TODO long?
}
