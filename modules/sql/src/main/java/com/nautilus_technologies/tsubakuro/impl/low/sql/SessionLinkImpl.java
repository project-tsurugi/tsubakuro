package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.PrepareDistiller;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.nautilus_technologies.tsubakuro.protos.ExecuteQueryDistiller;
import com.nautilus_technologies.tsubakuro.protos.BeginDistiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

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
    public Future<PreparedStatement> send(RequestProtos.Prepare.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return new FuturePreparedStatementImpl(wire.<ResponseProtos.Prepare>send(RequestProtos.Request.newBuilder().setPrepare(request), new PrepareDistiller()), this);
    };

    /**
     * Send execute sql statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setExecuteStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send execute prepared statement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setExecutePreparedStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send execute sql query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ExecuteQuery>send(RequestProtos.Request.newBuilder().setExecuteQuery(request), new ExecuteQueryDistiller());
    };

    /**
     * Send execute prepared query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire
    */
    public Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ExecuteQuery>send(RequestProtos.Request.newBuilder().setExecutePreparedQuery(request), new ExecuteQueryDistiller());
    };

    /**
     * Send begin request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    public Future<ResponseProtos.Begin> send(RequestProtos.Begin.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.Begin>send(RequestProtos.Request.newBuilder().setBegin(request), new BeginDistiller());
    };

    /**
     * Send commit request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setCommit(request), new ResultOnlyDistiller());
    };

    /**
     * Send rollback request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setRollback(request), new ResultOnlyDistiller());
    };

    /**
     * Send disposePreparedStatement request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setDisposePreparedStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send Disconnect request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    public Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.<ResponseProtos.ResultOnly>send(RequestProtos.Request.newBuilder().setDisconnect(request), new ResultOnlyDistiller());
    };

    public ResultSetWire createResultSetWire(String name) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.createResultSetWire(name);
    }

    /**
     * Close the SessionLinkImpl
     */
    public void close() throws IOException {
	wire.close();
	wire = null;
    }
}
