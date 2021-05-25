package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;
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

    static native long openNative(String name);
    static native void sendNative(long handle, ByteBuffer buffer);
    static native ByteBuffer recvNative(long handle);
    static native boolean closeNative(long handle);

    WireImpl(String name) throws IOException {
	wireHandle = openNative(name);
	if (wireHandle == 0) { throw new IOException(); }	    
    }
    public void close() throws IOException {
	if(wireHandle != 0) {
	    if(!closeNative(wireHandle)) { throw new IOException(); }
	    wireHandle = 0;
	}
    }
    /**
     * Send RequestProtos.Request to the SQL server via the native wire.
     @param request the RequestProtos.Request message
    */
    public void send(RequestProtos.Request request) throws IOException {
	if(wireHandle != 0) {
	    sendNative(wireHandle, ByteBuffer.wrap(request.toByteArray()));
	} else {
	    throw new IOException();
	}
    }
    /**
     * Receive ResponseProtos.Request from the SQL server via the native wire.
     @returns the ResposeProtos.Response message
    */
    public ResponseProtos.Response recv() throws IOException {
	try {
	    return ResponseProtos.Response.parseFrom(recvNative(wireHandle));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException();
	}
    }
}
