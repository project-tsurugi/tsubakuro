package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * WireImpl type.
 */
public class WireImpl implements Closeable {
    private long wireHandle = 0;  // for c++

    private static native long openNative(String name);
    private static native void sendNative(long handle, byte[] buffer);
    private static native byte[] recvNative(long handle);
    private static native boolean closeNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    WireImpl(String name) throws IOException {
	wireHandle = openNative(name);
	if (wireHandle == 0) {
	    throw new IOException("error: WireImpl.WireImpl()");
	}
    }

    public void close() throws IOException {
	if (wireHandle != 0) {
	    if (!closeNative(wireHandle)) {
		throw new IOException("error: WireImpl.close()");
	    }
	    wireHandle = 0;
	}
    }

    /**
     * Send RequestProtos.Request to the SQL server via the native wire.
     @param request the RequestProtos.Request message
    */
    public void send(RequestProtos.Request request) throws IOException {
	if (wireHandle != 0) {
	    sendNative(wireHandle, request.toByteArray());
	} else {
	    throw new IOException("error: WireImpl.send()");
	}
    }
    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     @returns ResposeProtos.Response message
    */
    public ResponseProtos.Response recv() throws IOException {
	try {
	    return ResponseProtos.Response.parseFrom(recvNative(wireHandle));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    IOException newEx = new IOException("error: WireImpl.recv()");
	    newEx.initCause(e);
	    throw newEx;
	}
    }
}
