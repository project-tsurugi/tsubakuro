package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements SessionWire {
    private long wireHandle = 0;  // for c++

    private static native long openNative(String name);
    private static native long sendNative(long handle, byte[] buffer);
    private static native byte[] recvNative(long handle);
    private static native boolean closeNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    public SessionWireImpl(String name) throws IOException {
	wireHandle = openNative(name);
	if (wireHandle == 0) {
	    throw new IOException("error: SessionWireImpl.SessionWireImpl()");
	}
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
	if (wireHandle != 0) {
	    if (!closeNative(wireHandle)) {
		throw new IOException("error: SessionWireImpl.close()");
	    }
	    wireHandle = 0;
	}
    }

    /**
     * Send RequestProtos.Request to the SQL server via the native wire.
     @param request the RequestProtos.Request message
    */
    public <V> FutureResponse<V> send(RequestProtos.Request request, FutureResponse.Distiller<V> distiller) throws IOException {
	if (wireHandle != 0) {
	    long handle = sendNative(wireHandle, request.toByteArray());
	    return new FutureResponseImpl<V>(this, distiller, new ResponseHandleImpl(handle));
	} else {
	    throw new IOException("error: SessionWireImpl.send()");
	}
    }
    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @returns ResposeProtos.Response message
    */
    public ResponseProtos.Response recv(ResponseHandle handle) throws IOException {
	try {
	    return ResponseProtos.Response.parseFrom(recvNative(((ResponseHandleImpl) handle).getHandle()));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    IOException newEx = new IOException("error: SessionWireImpl.recv()");
	    newEx.initCause(e);
	    throw newEx;
	}
    }

    public ResultSetWire createResultSetWire(String name) throws IOException {
	return new ResultSetWireImpl(wireHandle, name);
    }
}
