package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionLinkImpl type.
 */
public class LinkImpl implements Closeable {
    private long linkHandle;  // for c++

    static native long openNative(String name);
    static native void sendNative(long handle, ByteBuffer buffer);
    static native ByteBuffer recvNative(long handle);
    static native boolean closeNative(long handle);

    LinkImpl(String name) throws IOException {
	linkHandle = openNative(name);
	if (linkHandle == 0) { throw new IOException(); }	    
    }
    public void close() throws IOException {
	if(linkHandle != 0) {
	    if(!closeNative(linkHandle)) { throw new IOException(); }
	    linkHandle = 0;
	}
    }
    /**
     * Send RequestProtos.Request to the SQL server via the native link.
     @param request the RequestProtos.Request message
    */
    public void send(RequestProtos.Request request) throws IOException {
	if(linkHandle != 0) {
	    sendNative(linkHandle, ByteBuffer.wrap(request.toByteArray()));
	} else {
	    throw new IOException();
	}
    }
    /**
     * Receive ResponseProtos.Request from the SQL server via the native link.
     @returns the ResposeProtos.Response message
    */
    public ResponseProtos.Response recv() throws IOException {
	try {
	    return ResponseProtos.Response.parseFrom(recvNative(linkHandle));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException();
	}
    }
}
