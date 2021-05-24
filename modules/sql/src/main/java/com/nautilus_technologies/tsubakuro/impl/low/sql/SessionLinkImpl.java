package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionLinkImpl type.
 */
public class SessionLinkImpl implements SessionLink {
    private LinkImpl link;
    
    abstract private class FutureReceiver<V> implements Future<V> {
	private boolean _isCancelled = false;
	private boolean _isDone = false;

	public boolean cancel(boolean mayInterruptIfRunning) { _isCancelled = true; return true; }
	abstract public V get();
	public V get(long timeout, TimeUnit unit) { return get(); }
	public boolean isCancelled() { return _isCancelled; }
	public boolean isDone() { return _isDone; }
    }

    public class PrepareReceiver extends FutureReceiver<ResponseProtos.Prepare> {
	public ResponseProtos.Prepare get() {
	    try {
		ResponseProtos.Response response = ResponseProtos.Response.parseFrom(link.recv());
		return response.getPrepare();
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    public class ResultOnlyReceiver extends FutureReceiver<ResponseProtos.ResultOnly> {
	public ResponseProtos.ResultOnly get() {
	    try {
		ResponseProtos.Response response = ResponseProtos.Response.parseFrom(link.recv());
		return response.getResultOnly();
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    public class ExecuteQueryReceiver extends FutureReceiver<ResponseProtos.ExecuteQuery> {
	public ResponseProtos.ExecuteQuery get() {
	    try {
		ResponseProtos.Response response = ResponseProtos.Response.parseFrom(link.recv());
		return response.getExecuteQuery();
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    public class BeginReceiver extends FutureReceiver<ResponseProtos.Begin> {
	public ResponseProtos.Begin get() {
	    try {
		ResponseProtos.Response response = ResponseProtos.Response.parseFrom(link.recv());
		return response.getBegin();
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    /**
     * SendRequest RequestProtos.Request request to the SQL server via the link.
     @param request the RequestProtos.Request message
    */
    private void sendRequest(RequestProtos.Request request) {
	link.send(ByteBuffer.wrap(request.toByteArray()));
    }
    
    /**
     * Send prepare request to the SQL server via sendRequest().
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Prepare> contains prepared statement handle
    */
    public Future<ResponseProtos.Prepare> send(RequestProtos.Prepare request) {
	sendRequest(RequestProtos.Request.newBuilder().setPrepare(request).build());
	return new PrepareReceiver();
    };

    /**
     * Send execute sql statement request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement request) {
	sendRequest(RequestProtos.Request.newBuilder().setExecuteStatement(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send execute prepared statement request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement request) {
	sendRequest(RequestProtos.Request.newBuilder().setExecutePreparedStatement(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send execute sql query request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery request) {
	sendRequest(RequestProtos.Request.newBuilder().setExecuteQuery(request).build());
	return new ExecuteQueryReceiver();
    };

    /**
     * Send execute prepared query request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery request) {
	sendRequest(RequestProtos.Request.newBuilder().setExecutePreparedQuery(request).build());
	return new ExecuteQueryReceiver();
    };

    /**
     * Send begin request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    public Future<ResponseProtos.Begin> send(RequestProtos.Begin request) {
	sendRequest(RequestProtos.Request.newBuilder().setBegin(request).build());
	return new BeginReceiver();
    };

    /**
     * Send commit request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit request) {
	sendRequest(RequestProtos.Request.newBuilder().setCommit(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send rollback request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback request) {
	sendRequest(RequestProtos.Request.newBuilder().setRollback(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send disposePreparedStatement request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement request) {
	sendRequest(RequestProtos.Request.newBuilder().setDisposePreparedStatement(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send Disconnect request to via sendRequest()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect request) {
	sendRequest(RequestProtos.Request.newBuilder().setDisconnect(request).build());
	return new ResultOnlyReceiver();
    };
}
