package com.tsurugidb.tsubakuro.kvs.impl;

import com.tsurugidb.tsubakuro.kvs.TransactionHandle;

/**
 * An implementation of the {@link TransactionHandle}.
 */
public class TransactionHandleImpl implements TransactionHandle {

    private final long systemId;

    /**
     * Creates a new instance.
     * @param systemId system Id of this handle got by KvsResponse.Begin
     */
    public TransactionHandleImpl(long systemId) {
        this.systemId = systemId;
    }

    /**
     * retrieve the system id of this handle.
     * @return system id
     */
    public long getSystemId() {
        return systemId;
    }
}
