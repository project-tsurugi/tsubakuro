package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionLinkImpl type.
 */
public class SessionLinkImpl {
    private SessionWire wire;
    
    /**
     * Creates a new instance.
     * @param w the wire responsible for the communication conducted by this session
     */
    public SessionLinkImpl(SessionWire wire) {
	this.wire = wire;
    }

    /**
     * Send prepare request to the SQL server via wire.send().
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Prepare> contains prepared statement handle
    */
    public Future<PreparedStatement> send(RequestProtos.Prepare request) throws IOException {
	return new FuturePreparedStatementImpl(wire.<ResponseProtos.Prepare>send(RequestProtos.Request.newBuilder().setPrepare(request), new PrepareDistiller()));
    };

    /**
     * Send execute sql statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement request) throws IOException {
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setExecuteStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send execute prepared statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement request) throws IOException {
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setExecutePreparedStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send execute sql query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery request) throws IOException {
	return wire.<ResponseProtos.ExecuteQuery>send(RequestProtos.Request.newBuilder().setExecuteQuery(request), new ExecuteQueryDistiller());
    };

    /**
     * Send execute prepared query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery request) throws IOException {
	return wire.<ResponseProtos.ExecuteQuery>send(RequestProtos.Request.newBuilder().setExecutePreparedQuery(request), new ExecuteQueryDistiller());
    };

    /**
     * Send begin request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    public Future<ResponseProtos.Begin> send(RequestProtos.Begin request) throws IOException {
	return wire.<ResponseProtos.Begin>send(RequestProtos.Request.newBuilder().setBegin(request), new BeginDistiller());
    };

    /**
     * Send commit request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit request) throws IOException {
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setCommit(request), new ResultOnlyDistiller());
    };

    /**
     * Send rollback request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback request) throws IOException {
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setRollback(request), new ResultOnlyDistiller());
    };

    /**
     * Send disposePreparedStatement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement request) throws IOException {
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setDisposePreparedStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send Disconnect request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect request) throws IOException {
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setDisconnect(request), new ResultOnlyDistiller());
    };

    public ResultSetWire createResultSetWire(String name) throws IOException {
	return wire.createResultSetWire(name);
    }
}
