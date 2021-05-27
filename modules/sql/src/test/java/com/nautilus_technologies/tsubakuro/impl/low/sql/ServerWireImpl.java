package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * ServerWireImpl type.
 */
public class ServerWireImpl implements Closeable {
    private long wireHandle = 0;  // for c++

    static native long createNative(String name);
    static native byte[] getNative(long handle);
    static native void putNative(long handle, byte[] buffer);
    static native boolean closeNative(long handle);

    ServerWireImpl(String name) throws IOException {
	System.loadLibrary("wire-test");
	wireHandle = createNative(name);
	if (wireHandle == 0) {
	    throw new IOException();
	}
    }

    public void close() throws IOException {
	if (wireHandle != 0) {
	    if (!closeNative(wireHandle)) {
		throw new IOException();
	    }
	    wireHandle = 0;
	}
    }

    /**
     * Get RequestProtos.Request from a client via the native wire.
     @returns RequestProtos.Request
    */
    public RequestProtos.Request get() throws IOException {
	try {
	    return RequestProtos.Request.parseFrom(getNative(wireHandle));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException();
	}
    }

    /**
     * Put ResponseProtos.Response to the client via the native wire.
     @param request the ResponseProtos.Response message
    */
    public void put(ResponseProtos.Response response) throws IOException {
	if (wireHandle != 0) {
	    putNative(wireHandle, response.toByteArray());
	} else {
	    throw new IOException();
	}
    }
}
