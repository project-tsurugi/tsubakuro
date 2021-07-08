package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements SessionWire {
    private long wireHandle = 0;  // for c++
    private String dbName;
    private long sessionID;
    
    private static native long openNative(String name) throws IOException;
    private static native long sendNative(long sessionHandle, byte[] buffer);
    private static native ByteBuffer receiveNative(long responseHandle);
    private static native void releaseNative(long responseHandle);
    private static native void closeNative(long sessionHandle);

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
	closeNative(wireHandle);
	wireHandle = 0;
    }

    /**
     * Send RequestProtos.Request to the SQL server via the native wire.
     @param request the RequestProtos.Request message
    */
    public <V> Future<V> send(RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
	if (wireHandle == 0) {
	    throw new IOException("already closed");
	}
	var req = request.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(sessionID)).build().toByteArray();
	long handle = sendNative(wireHandle, req);
	return new FutureResponseImpl<V>(this, distiller, new ResponseWireHandleImpl(handle));
    }

    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @returns ResposeProtos.Response message
    */
    public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
	if (wireHandle == 0) {
	    throw new IOException("already closed");
	}
	try {
	    var responseHandle = ((ResponseWireHandleImpl) handle).getHandle();
	    var response = ResponseProtos.Response.parseFrom(receiveNative(responseHandle));
	    releaseNative(responseHandle);
	    return response;
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException("error: SessionWireImpl.receive()", e);
	}
    }

    /**
     * Create a ResultSetWire with the given name.
     @param name the name of the ResultSetWire to be created, where name must be unique within a session
     @returns ResultSetWireImpl
    */
    public ResultSetWire createResultSetWire(String name) throws IOException {
	if (wireHandle == 0) {
	    throw new IOException("already closed");
	}
	return new ResultSetWireImpl(wireHandle, name);
    }

    public String getDbName() {
	return dbName;
    }
}
