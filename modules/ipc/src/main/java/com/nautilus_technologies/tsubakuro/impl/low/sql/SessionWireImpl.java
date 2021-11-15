package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
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
    private static native long sendNative(long sessionHandle, byte[] buffer) throws IOException;
    private static native long sendQueryNative(long sessionHandle, byte[] buffer) throws IOException;
    private static native ByteBuffer receiveNative(long responseHandle);
    private static native void unReceiveNative(long responseHandle);
    private static native void releaseNative(long responseHandle);
    private static native void closeNative(long sessionHandle);

    static {
	System.loadLibrary("wire");
    }

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param dbName the name of the SQL server to which this SessionWireImpl is to be connected
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     */
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
     @returns a Future response message corresponding the request
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
     * Send RequestProtos.Request to the SQL server via the native wire.
     @param request the RequestProtos.Request message
     @returns a couple of Future response message corresponding the request
    */
    public Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> sendQuery(RequestProtos.Request.Builder request) throws IOException {
	if (wireHandle == 0) {
	    throw new IOException("already closed");
	}
	var req = request.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(sessionID)).build().toByteArray();
	long handle = sendQueryNative(wireHandle, req);
	return Pair.of(new FutureQueryResponseImpl(this, new ResponseWireHandleImpl(handle)),
		       new FutureResponseImpl<ResponseProtos.ResultOnly>(this, new ResultOnlyDistiller(), new ResponseWireHandleImpl(handle)));
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
     * UnReceive one ResponseProtos.Response
     @param handle the handle to the response box
    */
    public void unReceive(ResponseWireHandle handle) throws IOException {
	if (wireHandle == 0) {
	    throw new IOException("already closed");
	}
	unReceiveNative(((ResponseWireHandleImpl) handle).getHandle());
    }

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     @returns ResultSetWireImpl
    */
    public ResultSetWire createResultSetWire() throws IOException {
	if (wireHandle == 0) {
	    throw new IOException("already closed");
	}
	return new ResultSetWireImpl(wireHandle);
    }

    public String getDbName() {
	return dbName;
    }
}
