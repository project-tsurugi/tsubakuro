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
    private WireImpl wire;
    
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
		return wire.recv().getPrepare();
	    } catch (IOException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    public class ResultOnlyReceiver extends FutureReceiver<ResponseProtos.ResultOnly> {
	public ResponseProtos.ResultOnly get() {
	    try {
		return wire.recv().getResultOnly();
	    } catch (IOException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    public class ExecuteQueryReceiver extends FutureReceiver<ResponseProtos.ExecuteQuery> {
	public ResponseProtos.ExecuteQuery get() {
	    try {
		return wire.recv().getExecuteQuery();
	    } catch (IOException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    public class BeginReceiver extends FutureReceiver<ResponseProtos.Begin> {
	public ResponseProtos.Begin get() {
	    try {
		return wire.recv().getBegin();
	    } catch (IOException e) {
		System.out.println("IOException");
	    }
	    return null;
	}
    }

    /**
     * Send prepare request to the SQL server via wire.send().
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Prepare> contains prepared statement handle
    */
    public Future<ResponseProtos.Prepare> send(RequestProtos.Prepare request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setPrepare(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new PrepareReceiver();
    };

    /**
     * Send execute sql statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setExecuteStatement(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ResultOnlyReceiver();
    };

    /**
     * Send execute prepared statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setExecutePreparedStatement(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ResultOnlyReceiver();
    };

    /**
     * Send execute sql query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setExecuteQuery(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ExecuteQueryReceiver();
    };

    /**
     * Send execute prepared query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setExecutePreparedQuery(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ExecuteQueryReceiver();
    };

    /**
     * Send begin request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    public Future<ResponseProtos.Begin> send(RequestProtos.Begin request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setBegin(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new BeginReceiver();
    };

    /**
     * Send commit request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setCommit(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ResultOnlyReceiver();
    };

    /**
     * Send rollback request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setRollback(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ResultOnlyReceiver();
    };

    /**
     * Send disposePreparedStatement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setDisposePreparedStatement(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ResultOnlyReceiver();
    };

    /**
     * Send Disconnect request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect request) {
	try {
	    wire.send(RequestProtos.Request.newBuilder().setDisconnect(request).build());
	} catch (IOException e) {
	    System.out.println("IOException");
	}
	return new ResultOnlyReceiver();
    };
}
