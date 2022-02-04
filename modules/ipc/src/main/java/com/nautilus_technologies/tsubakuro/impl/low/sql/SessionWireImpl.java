package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Objects;
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
    private Queue<QueueEntry> queue;
    
    private static native long openNative(String name) throws IOException;
    private static native long sendNative(long sessionHandle, byte[] buffer);
    private static native long sendQueryNative(long sessionHandle, byte[] buffer);
    private static native ByteBuffer receiveNative(long responseHandle);
    private static native ByteBuffer receiveNative(long responseHandle, long timeout) throws TimeoutException;
    private static native void unReceiveNative(long responseHandle);
    private static native void releaseNative(long responseHandle);
    private static native void closeNative(long sessionHandle);

    static {
	System.loadLibrary("wire");
    }

    enum RequestType {
	STATEMENT,
	QUERY
    };

    static class QueueEntry<V> {
	RequestType type;
	byte[] request;
	FutureResponseImpl<V> futureBody;
	FutureQueryResponseImpl futureHead;

	QueueEntry(byte[] request, FutureQueryResponseImpl futureHead, FutureResponseImpl<V> futureBody) {
	    this.type = RequestType.QUERY;
	    this.request = request;
	    this.futureBody = futureBody;
	    this.futureHead = futureHead;
	}
	QueueEntry(byte[] request, FutureResponseImpl<V> futureBody) {
	    this.type = RequestType.STATEMENT;
	    this.request = request;
	    this.futureBody = futureBody;
	}
	RequestType getRequestType() {
	    return type;
	}
	byte[] getRequest() {
	    return request;
	}
	FutureQueryResponseImpl getFutureHead() {
	    return futureHead;
	}
	FutureResponseImpl<V> getFutureBody() {
	    return futureBody;
	}
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
	this.queue = new ArrayDeque<>();
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
	var futureBody = new FutureResponseImpl<V>(this, distiller);
	long handle;
	synchronized (this) {
	    handle = sendNative(wireHandle, req);
	}
	if (handle != 0) {
	    futureBody.setResponseHandle(new ResponseWireHandleImpl(handle));
	} else {
	    queue.add(new QueueEntry<V>(req, futureBody));
	}
	return futureBody;
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
	var left = new FutureQueryResponseImpl(this);
	var right = new FutureResponseImpl<ResponseProtos.ResultOnly>(this, new ResultOnlyDistiller());
	long handle;
	synchronized (this) {
	    handle = sendQueryNative(wireHandle, req);
	}
	if (handle != 0) {
	    left.setResponseHandle(new ResponseWireHandleImpl(handle));
	    right.setResponseHandle(new ResponseWireHandleImpl(handle));
	} else {
	    queue.add(new QueueEntry<ResponseProtos.ResultOnly>(req, left, right));
	}
	return Pair.of(left, right);
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
	    synchronized (this) {
		releaseNative(responseHandle);
		var entry = queue.peek();
		if (!Objects.isNull(entry)) {
		    if (entry.getRequestType() == RequestType.STATEMENT) {
			long responseBoxHandle = sendNative(wireHandle, entry.getRequest());
			if (responseBoxHandle != 0) {
			    entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
			    queue.poll();
			}
		    } else {
			long responseBoxHandle = sendQueryNative(wireHandle, entry.getRequest());
			if (responseBoxHandle != 0) {
			    entry.getFutureHead().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
			    entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
			    queue.poll();
			}
		    }
		}
	    }
	    return response;
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException("error: SessionWireImpl.receive()", e);
	}
    }

    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @returns ResposeProtos.Response message
    */
    public ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
	if (wireHandle == 0) {
	    throw new IOException("already closed");
	}
	try {
	    var responseHandle = ((ResponseWireHandleImpl) handle).getHandle();
	    var timeoutNano = unit.toNanos(timeout);
	    if (timeoutNano == Long.MIN_VALUE) {
		throw new IOException("timeout duration overflow");
	    }
	    var response = ResponseProtos.Response.parseFrom(receiveNative(responseHandle, timeoutNano));
	    synchronized (this) {
		releaseNative(responseHandle);
		var entry = queue.peek();
		if (!Objects.isNull(entry)) {
		    if (entry.getRequestType() == RequestType.STATEMENT) {
			long responseBoxHandle = sendNative(wireHandle, entry.getRequest());
			if (responseBoxHandle != 0) {
			    entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
			    queue.poll();
			}
		    } else {
			long responseBoxHandle = sendQueryNative(wireHandle, entry.getRequest());
			if (responseBoxHandle != 0) {
			    entry.getFutureHead().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
			    entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
			    queue.poll();
			}
		    }
		}
	    }
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
