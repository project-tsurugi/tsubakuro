package com.nautilus_technologies.tsubakuro.impl.low.sql;

public class ResponseHandleImpl extends ResponseHandle {
    long handle;
    ResponseHandleImpl(long h) {
	handle = h;
    }
    long getHandle() {
	return handle;
    }
}
