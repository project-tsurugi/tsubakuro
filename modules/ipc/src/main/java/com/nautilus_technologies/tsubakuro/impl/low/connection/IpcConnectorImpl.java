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
    private static native long requestConnectionNative(String name);
    private static native boolean checkConnectionNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    private IpcConnectorImpl() {
	//not called (for spotbugs)
    }

    public static Future<SessionWire> connect(String name) throws IOException {
	long handle = requestConnectionNative(name);
	return new FutureSessionWireImpl(name, handle);
    }

    public static SessionWire getSessionWire(String name) throws IOException {
	return new SessionWireImpl(name);
    }

    public static boolean checkConnection(long handle) {
	return checkConnectionNative(handle);
    }
}
