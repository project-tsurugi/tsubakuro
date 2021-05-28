package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionLinkImpl type.
 */
public class SessionLinkImpl implements SessionLink {
    private WireImpl wire;
    
    public SessionLinkImpl(WireImpl w) {
	wire = w;
    }

    abstract private class FutureReceiver<V> implements Future<V> {
	private boolean isCancelled = false;
	private boolean isDone = false;

	public boolean cancel(boolean mayInterruptIfRunning) { isCancelled = true; return true; }
	public abstract V get() throws ExecutionException;
	public V get(long timeout, TimeUnit unit) throws ExecutionException { return get(); }
	public boolean isCancelled() { return isCancelled; }
	public boolean isDone() { return isDone; }
    }

    class PrepareReceiver extends FutureReceiver<ResponseProtos.Prepare> {
	public ResponseProtos.Prepare get() throws ExecutionException {
	    try {
		return wire.recv().getPrepare();
	    } catch (IOException e) {
		throw new ExecutionException(e);
	    }
	}
    }

    class ResultOnlyReceiver extends FutureReceiver<ResponseProtos.ResultOnly> {
	public ResponseProtos.ResultOnly get() throws ExecutionException {
	    try {
		return wire.recv().getResultOnly();
	    } catch (IOException e) {
		throw new ExecutionException(e);
	    }
	}
    }

    class ExecuteQueryReceiver extends FutureReceiver<ResponseProtos.ExecuteQuery> {
	public ResponseProtos.ExecuteQuery get() throws ExecutionException {
	    try {
		return wire.recv().getExecuteQuery();
	    } catch (IOException e) {
		throw new ExecutionException(e);
	    }
	}
    }

    class BeginReceiver extends FutureReceiver<ResponseProtos.Begin> {
	public ResponseProtos.Begin get() throws ExecutionException {
	    try {
		return wire.recv().getBegin();
	    } catch (IOException e) {
		throw new ExecutionException(e);
	    }
	}
    }

    /**
     * Send prepare request to the SQL server via wire.send().
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Prepare> contains prepared statement handle
    */
    public Future<ResponseProtos.Prepare> send(RequestProtos.Prepare request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setPrepare(request).build());
	return new PrepareReceiver();
    };

    /**
     * Send execute sql statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setExecuteStatement(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send execute prepared statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setExecutePreparedStatement(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send execute sql query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setExecuteQuery(request).build());
	return new ExecuteQueryReceiver();
    };

    /**
     * Send execute prepared query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setExecutePreparedQuery(request).build());
	return new ExecuteQueryReceiver();
    };

    /**
     * Send begin request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    public Future<ResponseProtos.Begin> send(RequestProtos.Begin request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setBegin(request).build());
	return new BeginReceiver();
    };

    /**
     * Send commit request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setCommit(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send rollback request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setRollback(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send disposePreparedStatement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setDisposePreparedStatement(request).build());
	return new ResultOnlyReceiver();
    };

    /**
     * Send Disconnect request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect request) throws IOException {
	wire.send(RequestProtos.Request.newBuilder().setDisconnect(request).build());
	return new ResultOnlyReceiver();
    };
}
