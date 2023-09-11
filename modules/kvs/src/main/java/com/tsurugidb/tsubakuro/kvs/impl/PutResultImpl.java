package com.tsurugidb.tsubakuro.kvs.impl;

import java.text.MessageFormat;

import com.tsurugidb.tsubakuro.kvs.PutResult;

/**
 * An implementation of {@link PutResult}.
 */
public class PutResultImpl implements PutResult {

    private final int written;

    /**
     * Creates a new instance.
     * @param written the number of records which the operation actually written.
     */
    public PutResultImpl(int written) {
        if (written < 0) {
            throw new IllegalArgumentException(
                    MessageFormat.format("written count is negative: {}", written));
        }
        this.written = written;
    }

    @Override
    public int size() {
        return written;
    }

}
