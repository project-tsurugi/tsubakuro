package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a result of {@code PUT} request.
 */
public interface PutResult {

    /**
     * Returns the number of records which the operation actually written.
     * @return the number of records
     */
    int size(); // TODO long?
}
