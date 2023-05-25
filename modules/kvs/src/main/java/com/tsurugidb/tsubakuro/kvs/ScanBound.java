package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a scan bound.
 */
public enum ScanBound {

    /**
     * The scan range includes the key.
     */
    INCLUSIVE,

    /**
     * The scan range does not include the key.
     */
    EXCLUSIVE,
}
