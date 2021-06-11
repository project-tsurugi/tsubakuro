package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements SessionWire {
    private long wireHandle = 0;  // for c++
    private String dbName;
    private long sessionID;
    
    private static native long openNative(String name) throws IOException;
    private static native long sendNative(long handle, ByteBuffer buffer);
    private static native ByteBuffer receiveNative(long handle);
    private static native boolean closeNative(long handle);

    static {
	System.loadLibrary("wire");
    }

    public SessionWireImpl(String dbName, long sessionID) throws IOException {
	wireHandle = openNative(dbName + "-" + String.valueOf(sessionID));
	this.dbName = dbName;
	this.sessionID = sessionID;
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
    public <V> Future<V> send(RequestProtos.Request request, Distiller<V> distiller) throws IOException {
	if (wireHandle != 0) {
	    var req = request.toByteString();
	    ByteBuffer buffer = ByteBuffer.allocateDirect(req.size());
	    req.copyTo(buffer);
	    long handle = sendNative(wireHandle, buffer);
	    return new FutureResponseImpl<V>(this, distiller, new ResponseWireHandleImpl(handle));
	} else {
	    throw new IOException("error: SessionWireImpl.send()");
	}
    }
    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @returns ResposeProtos.Response message
    */
    public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
	try {
	    return ResponseProtos.Response.parseFrom(receiveNative(((ResponseWireHandleImpl) handle).getHandle()));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException("error: SessionWireImpl.receive()", e);
	}
    }

    public ResultSetWire createResultSetWire(String name) throws IOException {
	return new ResultSetWireImpl(wireHandle, name);
    }

    public String getDbName() {
	return dbName;
    }

    public long getSessionID() {
	return sessionID;
    }
}
