package com.nautilus_technologies.tsubakuro.impl.low.connection;

import java.io.Closeable;
import java.io.IOException;

/**
 * ServerConnectionImpl type.
 */
public class ServerConnectionImpl implements Closeable {
    private static native long listenNative(String name);
    private static native long acceptNative(long handle);
    private static native void closeNative(long handle);

    static {
	System.loadLibrary("wire-test");
    }

    private long handle;

    ServerConnectionImpl(String name) throws IOException {
	this.handle = listenNative(name);
    }

    public long accept() {
	return acceptNative(handle);
    }

    public void close() throws IOException {
	closeNative(handle);
    }
}
