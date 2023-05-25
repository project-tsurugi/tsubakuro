package com.tsurugidb.tsubakuro.kvs.impl;

import com.tsurugidb.tsubakuro.kvs.RemoveResult;

/**
 * An implementation of {@link RemoveResult}.
 */
public class RemoveResultImpl implements RemoveResult {

    private final int removed;

    /**
     * Creates a new instance.
     * @param removed the number of records which the operation actually removed
     */
    public RemoveResultImpl(int removed) {
        this.removed = removed;
    }

    @Override
    public int size() {
        return removed;
    }

}
