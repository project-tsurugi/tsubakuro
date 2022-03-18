package com.nautilus_technologies.tsubakuro.channel.ipc.connection;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.ipc.sql.SessionWireImpl;

/**
 * IpcConnectorImpl type.
 */
public final class IpcConnectorImpl implements Connector {
    private static native long getConnectorNative(String name) throws IOException;
    private static native long requestNative(long handle);
    private static native void waitNative(long handle, long id);
    private static native void waitNative(long handle, long id, long timeout) throws TimeoutException;
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
	waitNative(handle, id);
	close();
	return new SessionWireImpl(name, id);
    }

    public SessionWire getSessionWire(long timeout, TimeUnit unit) throws TimeoutException, IOException {
	var timeoutNano = unit.toNanos(timeout);
	if (timeoutNano == Long.MIN_VALUE) {
	    throw new IOException("timeout duration overflow");
	}
	waitNative(handle, id, timeoutNano);
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
