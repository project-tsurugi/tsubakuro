package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a behavior of {@code PUT} operation.
 */
public enum PutType {

    /**
     * Puts a table entry in any case (default behavior).
     */
    OVERWRITE,

    /**
     * Puts a table entry only if it is absent.
     */
    IF_ABSENT,

    /**
     * Puts a table entry only if it is already exists.
     */
    IF_PRESENT,

    ;

    /**
     * The default behavior of {@code PUT} operation.
     * @see #OVERWRITE
     */
    public static final PutType DEFAULT_BEHAVIOR = OVERWRITE;
}
