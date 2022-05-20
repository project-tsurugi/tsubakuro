package com.nautilus_technologies.tsubakuro.channel.stream;

import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;

/**
 * ResponseWireHandleImpl type, where the type of the handle that this class stores is StreamWire.
 */
public class ResponseWireHandleImpl extends ResponseWireHandle {
    private byte index;

    /**
     * Class constructor, called from SessionWireImpl that is connected to the SQL server.
     * @param handle the handle that this class stores.
     */
    ResponseWireHandleImpl(byte index) {
    this.index = index;
    }
    /**
     * Provides the handle.
     * @return the handle in StreamWire, where the type is implementation-dependent
     */
    byte getHandle() {
    return index;
    }
}
