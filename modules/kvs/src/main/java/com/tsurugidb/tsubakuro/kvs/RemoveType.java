package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a behavior of {@code REMOVE} operation.
 */
public enum RemoveType {

    /**
     * Removes table entries with counting the actually removed entries (default behavior).
     * @see RemoveResult#size()
     */
    COUNTING,

    /**
     * Removes table entries without counting the removed entries.
     * <p>
     * Using this, {@link RemoveResult#size()} will return {@code 0}.
     * </p>
     */
    QUIET,
    ;

    /**
     * The default behavior of {@code REMOVE} operation.
     * @see #COUNTING
     */
    public static final RemoveType DEFAULT_BEHAVIOR = COUNTING;
}
