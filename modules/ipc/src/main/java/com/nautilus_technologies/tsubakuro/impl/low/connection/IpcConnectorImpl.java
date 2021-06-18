package com.nautilus_technologies.tsubakuro.impl.low.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWireImpl;

/**
 * IpcConnectorImpl type.
 */
public final class IpcConnectorImpl implements Connector {
    private static native long getConnectorNative(String name) throws IOException;
    private static native long requestNative(long handle);
    private static native boolean checkNative(long handle, long id);
    private static native void closeConnectorNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    String name;
    long handle;
    long id;
    
    public IpcConnectorImpl(String name) {
	this.name = name;
    }
    
    public Future<SessionWire> connect() throws IOException {
	handle = getConnectorNative(name);
	id = requestNative(handle);
	return new FutureSessionWireImpl(this);
    }

    public SessionWire getSessionWire() throws IOException {
	close();
	return new SessionWireImpl(name, id);
    }

    public boolean checkConnection() {
	return checkNative(handle, id);
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
	if (handle != 0) {
	    closeConnectorNative(handle);
	}
	handle = 0;
    }
}
