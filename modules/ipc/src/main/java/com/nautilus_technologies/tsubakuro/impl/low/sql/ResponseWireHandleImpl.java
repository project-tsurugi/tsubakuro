package com.nautilus_technologies.tsubakuro.impl.low.sql;

/**
 * ResponseWireHandleImpl type, where the type of the handle that this class stores is long.
 */
public class ResponseWireHandleImpl extends ResponseWireHandle {
    private long handle;

    /**
     * Creates a new instance.
     * @param h the handle that this class stores.
     */
    ResponseWireHandleImpl(long handle) {
	this.handle = handle;
    }
    /**
     * Returns the handle.
     * @return the handle
     */
    long getHandle() {
	return handle;
    }
}
