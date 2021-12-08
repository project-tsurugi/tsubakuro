package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.PrepareDistiller;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.nautilus_technologies.tsubakuro.protos.BeginDistiller;
import com.nautilus_technologies.tsubakuro.protos.ExplainDistiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * SessionLinkImpl type.
 */
public class SessionLinkImpl {
    private SessionWire wire;
    
    /**
     * Class constructor, called from SessionImpl
     * @param sessionWire the wire that connects to the Database
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
     * Send explain request to the SQL server via wire.send().
     @param request the request message encoded with protocol buffer
     @return Future<String> contains a string to explain the plan
    */
    public Future<String> send(RequestProtos.Explain.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return new FutureExplainImpl(wire.<ResponseProtos.Explain>send(RequestProtos.Request.newBuilder().setExplain(request), new ExplainDistiller()));
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
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire and record metadata,
     and Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not.
    */
    public Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> send(RequestProtos.ExecuteQuery.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.sendQuery(RequestProtos.Request.newBuilder().setExecuteQuery(request));
    };

    /**
     * Send execute prepared query request to via wire.send()
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set wire and record metadata,
     and Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not.
    */
    public Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> send(RequestProtos.ExecutePreparedQuery.Builder request) throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.sendQuery(RequestProtos.Request.newBuilder().setExecutePreparedQuery(request));
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

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     @return a resultSetWire
    */
    public ResultSetWire createResultSetWire() throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	return wire.createResultSetWire();
    }

    /**
     * Close the SessionLinkImpl
     */
    public void close() throws IOException {
	if (Objects.isNull(wire)) {
	    throw new IOException("already closed");
	}
	wire.close();
	wire = null;
    }
}
