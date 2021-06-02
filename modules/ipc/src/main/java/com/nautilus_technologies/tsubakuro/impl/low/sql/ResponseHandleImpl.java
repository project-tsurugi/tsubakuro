package com.nautilus_technologies.tsubakuro.impl.low.sql;

/**
 * ResponseHandleImpl type, where the type of the handle that this class stores is long.
 */
public class ResponseHandleImpl extends ResponseHandle {
    private long handle;

    /**
     * Creates a new instance.
     * @param h the handle that this class stores.
     */
    ResponseHandleImpl(long h) {
	handle = h;
    }
    /**
     * Returns the handle.
     * @return the handle
     */
    long getHandle() {
	return handle;
    }
}
