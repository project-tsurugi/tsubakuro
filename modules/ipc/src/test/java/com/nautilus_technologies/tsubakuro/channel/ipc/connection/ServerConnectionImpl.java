package com.nautilus_technologies.tsubakuro.channel.ipc.connection;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.ipc.sql.ServerWireImpl;

/**
 * ServerConnectionImpl type.
 */
public class ServerConnectionImpl implements Closeable {
    private static native long createNative(String name);
    private static native long listenNative(long handle);
    private static native void acceptNative(long handle, long id);
    private static native void closeNative(long handle);

    static {
	System.loadLibrary("wire-test");
    }

    private long handle;
    private String name;

    ServerConnectionImpl(String name) throws IOException {
	this.handle = createNative(name);
	this.name = name;
    }

    public long listen() {
	return listenNative(handle);
    }

    public ServerWireImpl accept(long id) throws IOException {
	var rv = new ServerWireImpl(name, id);
	acceptNative(handle, id);
	return rv;
    }

    public void close() throws IOException {
	closeNative(handle);
    }
}
