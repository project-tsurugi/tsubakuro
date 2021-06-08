package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;

/**
 * ServerWireImpl type.
 */
public class ServerWireImpl implements Closeable {
    private long wireHandle = 0;  // for c++

    private static native long createNative(String name);
    private static native byte[] getNative(long handle);
    private static native void putNative(long handle, byte[] buffer);
    private static native boolean closeNative(long handle);
    private static native long createRSLNative(long handle, String name);
    private static native void putRSLNative(long handle, byte[] buffer);
    private static native long closeRSLNative(long handle);

    static {
	System.loadLibrary("wire-test");
    }

    ServerWireImpl(String name) throws IOException {
	wireHandle = createNative(name);
	if (wireHandle == 0) {
	    throw new IOException("error: ServerWireImpl.ServerWireImpl()");
	}
    }

    public void close() throws IOException {
	if (wireHandle != 0) {
	    if (!closeNative(wireHandle)) {
		throw new IOException("error: ServerWireImpl.close()");
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
	    throw new IOException("error: ServerWireImpl.get()");
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
	    throw new IOException("error: ServerWireImpl.put()");
	}
    }

    public long createRSL(String name) throws IOException {
	if (wireHandle != 0) {
	    return createRSLNative(wireHandle, name);
	} else {
	    throw new IOException("error: ServerWireImpl.createRSL()");
	}
    }

    public void putRSL(long handle, SchemaProtos.RecordMeta metadata) throws IOException {
	if (handle != 0) {
	    putRSLNative(handle, metadata.toByteArray());
	} else {
	    throw new IOException("error: ServerWireImpl.putRSL()");
	}
    }
}