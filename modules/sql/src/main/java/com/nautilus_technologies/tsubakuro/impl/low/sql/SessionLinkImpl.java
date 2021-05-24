package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Callable;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionLinkImpl type.
 */
public class SessionLinkImpl implements SessionLink {
    private LinkImpl link;
    
    private class PrepareCaller implements Callable<ResponseProtos.Prepare>  {
	public ResponseProtos.Prepare call() {
	    ResponseProtos.Prepare.Builder builder = ResponseProtos.Prepare.newBuilder();
	    return builder.build();
	}
    }
    private class ExecuteStatementCaller implements Callable<ResponseProtos.ResultOnly>  {
	public ResponseProtos.ResultOnly call() {
	    ResponseProtos.ResultOnly.Builder builder = ResponseProtos.ResultOnly.newBuilder();
	    return builder.build();
	}
    }
    private class ExecutePreparedStatementCaller implements Callable<ResponseProtos.ResultOnly>  {
	public ResponseProtos.ResultOnly call() {
	    ResponseProtos.ResultOnly.Builder builder = ResponseProtos.ResultOnly.newBuilder();
	    return builder.build();
	}
    }
    private class ExecuteQueryCaller implements Callable<ResponseProtos.ExecuteQuery>  {
	public ResponseProtos.ExecuteQuery call() {
	    ResponseProtos.ExecuteQuery.Builder builder = ResponseProtos.ExecuteQuery.newBuilder();
	    return builder.build();
	}
    }
    private class ExecutePreparedQueryCaller implements Callable<ResponseProtos.ExecuteQuery>  {
	public ResponseProtos.ExecuteQuery call() {
	    ResponseProtos.ExecuteQuery.Builder builder = ResponseProtos.ExecuteQuery.newBuilder();
	    return builder.build();
	}
    }
    private class BeginCaller implements Callable<ResponseProtos.Begin>  {
	public ResponseProtos.Begin call() {
	    ResponseProtos.Begin.Builder builder = ResponseProtos.Begin.newBuilder();
	    return builder.build();
	}
    }
    private class CommitCaller implements Callable<ResponseProtos.ResultOnly>  {
	public ResponseProtos.ResultOnly call() {
	    ResponseProtos.ResultOnly.Builder builder = ResponseProtos.ResultOnly.newBuilder();
	    return builder.build();
	}
    }
    private class RollbackCaller implements Callable<ResponseProtos.ResultOnly>  {
	public ResponseProtos.ResultOnly call() {
	    ResponseProtos.ResultOnly.Builder builder = ResponseProtos.ResultOnly.newBuilder();
	    return builder.build();
	}
    }
    private class DisposePreparedStatementCaller implements Callable<ResponseProtos.ResultOnly>  {
	public ResponseProtos.ResultOnly call() {
	    ResponseProtos.ResultOnly.Builder builder = ResponseProtos.ResultOnly.newBuilder();
	    return builder.build();
	}
    }
    private class DisconnectCaller implements Callable<ResponseProtos.ResultOnly>  {
	public ResponseProtos.ResultOnly call() {
	    ResponseProtos.ResultOnly.Builder builder = ResponseProtos.ResultOnly.newBuilder();
	    return builder.build();
	}
    }

    /**
     * Send prepare request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Prepare> contains prepared statement handle
    */
    public Future<ResponseProtos.Prepare> send(RequestProtos.Prepare request) {
	return new FutureTask<ResponseProtos.Prepare>(new PrepareCaller());
    };

    /**
     * Send execute sql statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement request) {
	return new FutureTask<ResponseProtos.ResultOnly>(new ExecuteStatementCaller());
    };

    /**
     * Send execute prepared statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement request) {
	return new FutureTask<ResponseProtos.ResultOnly>(new ExecutePreparedStatementCaller());
    };

    /**
     * Send execute sql query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery request) {
	return new FutureTask<ResponseProtos.ExecuteQuery>(new ExecuteQueryCaller());
    };

    /**
     * Send execute prepared query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery request) {
	return new FutureTask<ResponseProtos.ExecuteQuery>(new ExecutePreparedQueryCaller());
    };

    /**
     * Send begin request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    public Future<ResponseProtos.Begin> send(RequestProtos.Begin request) {
	return new FutureTask<ResponseProtos.Begin>(new BeginCaller());
    };

    /**
     * Send commit request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit request) {
	return new FutureTask<ResponseProtos.ResultOnly>(new CommitCaller());
    };

    /**
     * Send rollback request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback request) {
	return new FutureTask<ResponseProtos.ResultOnly>(new RollbackCaller());
    };

    /**
     * Send disposePreparedStatement request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement request) {
	return new FutureTask<ResponseProtos.ResultOnly>(new DisposePreparedStatementCaller());
    };

    /**
     * Send Disconnect request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect request) {
	return new FutureTask<ResponseProtos.ResultOnly>(new DisconnectCaller());
    };
}
