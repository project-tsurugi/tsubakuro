package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * WireImpl type.
 */
public class WireImpl implements Wire {
    private long wireHandle = 0;  // for c++

    private static native long openNative(String name);
    private static native long sendNative(long handle, byte[] buffer);
    private static native byte[] recvNative(long handle);
    private static native boolean closeNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    /**
     * Creates a new instance.
     * @param name the name of the wire, assumed that the connection name returned from the SQL service as a result of the connect operation is given
     */
    WireImpl(String name) throws IOException {
	wireHandle = openNative(name);
	if (wireHandle == 0) {
	    throw new IOException("error: WireImpl.WireImpl()");
	}
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
	if (wireHandle != 0) {
	    if (!closeNative(wireHandle)) {
		throw new IOException("error: WireImpl.close()");
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
	    return new FutureResponseImpl(this, distiller, new ResponseHandleImpl(handle));
	} else {
	    throw new IOException("error: WireImpl.send()");
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
	    IOException newEx = new IOException("error: WireImpl.recv()");
	    newEx.initCause(e);
	    throw newEx;
	}
    }
}
