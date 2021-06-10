package com.nautilus_technologies.tsubakuro.impl.low.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.connection.ConnectProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWireImpl;

/**
 * IpcConnectorImpl type.
 */
public final class IpcConnectorImpl {
    private static native long sendConnectRequestNative(String name);
    private static native ByteBuffer receiveConnectResponseNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    private IpcConnectorImpl() {
	//not called (for spotbugs)
    }

    public static Future<SessionWire> connect(String name) throws IOException {
	long handle = sendConnectRequestNative(name);
	return new FutureSessionWireImpl(handle);
    }

    public static SessionWire getSessionWire(long handle) throws IOException {
	var response = ConnectProtos.ConnectResponse.parseFrom(receiveConnectResponseNative(handle));
	if (ConnectProtos.ConnectResponse.ResultCase.ERROR.equals(response.getResultCase())) {
		throw new IOException(response.getError().getDetail());
	}
	return new SessionWireImpl(sessionName(response.getSession().getHandle()));
    }

    private static String sessionName(long sessionHandle) {
	return String.valueOf(sessionHandle);
    }
}
