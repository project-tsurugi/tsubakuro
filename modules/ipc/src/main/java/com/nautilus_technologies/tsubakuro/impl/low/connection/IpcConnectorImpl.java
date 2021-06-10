package com.nautilus_technologies.tsubakuro.impl.low.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWireImpl;

/**
 * IpcConnectorImpl type.
 */
public final class IpcConnectorImpl {
    private static native long getConnectorNative(String name);
    private static native long requestNative(long handle);
    private static native boolean checkNative(long handle, long id);
    private static native void closeConnectorNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    private IpcConnectorImpl() {
	//not called (for spotbugs)
    }

    public static Future<SessionWire> connect(String name) throws IOException {
	long handle = getConnectorNative(name);
	long id = requestNative(handle);
	return new FutureSessionWireImpl(name, handle, id);
    }

    public static SessionWire getSessionWire(String name, long handle, long id) throws IOException {
	closeConnectorNative(handle);
	return new SessionWireImpl(name + "-" + String.valueOf(id));
    }

    public static boolean checkConnection(long handle, long id) {
	return checkNative(handle, id);
    }
}
