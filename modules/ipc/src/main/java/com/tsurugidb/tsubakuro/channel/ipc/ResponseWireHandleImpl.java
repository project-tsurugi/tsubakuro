package com.tsurugidb.tsubakuro.channel.ipc;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseWireHandle;

/**
 * ResponseWireHandleImpl type, where the type of the handle that this class stores is long.
 */
public class ResponseWireHandleImpl extends ResponseWireHandle {
    private long handle;

    /**
     * Class constructor, called from SessionWireImpl that is connected to the SQL server.
     * @param handle the handle that this class stores.
     */
    ResponseWireHandleImpl(long handle) {
    this.handle = handle;
    }
    /**
     * Provides the handle.
     * @return the handle in long, where the type is implementation-dependent
     */
    long getHandle() {
    return handle;
    }
}
