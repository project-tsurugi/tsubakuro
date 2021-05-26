package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;
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
    static native ByteBuffer getNative(long handle);
    static native void putNative(long handle, ByteBuffer buffer);
    static native boolean closeNative(long handle);

    ServerWireImpl(String name) throws IOException {
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
    public void put(ResponseProtos.Response request) throws IOException {
	if (wireHandle != 0) {
	    putNative(wireHandle, ByteBuffer.wrap(request.toByteArray()));
	} else {
	    throw new IOException();
	}
    }
}
