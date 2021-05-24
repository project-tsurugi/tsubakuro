package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;
import java.io.Closeable;
import java.io.IOException;

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
	}
    }
    public void send(ByteBuffer buffer) {
	sendNative(linkHandle, buffer);
    }
    public ByteBuffer recv() {
	return recvNative(linkHandle);
    }
}
