package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a behavior of {@code SCAN} operation.
 */
public enum ScanType {

    /**
     * Scans over an index from the first entry to the last.
     */
    FORWARD,

    /**
     * Scans over an index from the last entry to the first.
     */
    BACKWARD,

    ;

    /**
     * The default behavior of {@code SCAN} operation.
     * @see #FORWARD
     */
    public static final ScanType DEFAULT_BEHAVIOR = FORWARD;
}
