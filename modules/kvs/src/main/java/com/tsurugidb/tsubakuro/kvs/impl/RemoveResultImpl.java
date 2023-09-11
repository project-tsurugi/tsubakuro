package com.tsurugidb.tsubakuro.kvs.impl;

import java.text.MessageFormat;

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
        if (removed < 0) {
            throw new IllegalArgumentException(
                    MessageFormat.format("removed count is negative: {}", removed));
        }
        this.removed = removed;
    }

    @Override
    public int size() {
        return removed;
    }

}
