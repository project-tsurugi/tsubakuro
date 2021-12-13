package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;
    
import org.junit.jupiter.api.Test;

class SessionLinkImplTest {
    ResponseProtos.Response nextResponse;
    
    class FutureResponseMock<V> implements Future<V> {
	private SessionWireMock wire;
	private Distiller<V> distiller;
	private ResponseWireHandle handle; // dummey
	FutureResponseMock(SessionWireMock wire, Distiller<V> distiller) {
	    this.wire = wire;
	    this.distiller = distiller;
	}
        public V get() throws ExecutionException {
	    try {
		var response = wire.receive(handle);
		if (Objects.isNull(response)) {
		    throw new IOException("received null response at FutureResponseMock, probably test program is incomplete");
		}
		return distiller.distill(response);
	    } catch (IOException e) {
		throw new ExecutionException(e);
	    }
	}
	public V get(long timeout, TimeUnit unit) throws ExecutionException {
	    return get();  // FIXME need to be implemented properly, same as below
	}
	public boolean isDone() {
	    return true;
	}
	public boolean isCancelled() {
	    return false;
	}
	public boolean cancel(boolean mayInterruptIfRunning) {
	    return false;
	}
    }

    class SessionWireMock implements SessionWire {
	public <V> Future<V> send(RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
	    switch (request.getRequestCase()) {
	    case BEGIN:
		nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
		return new FutureResponseMock<V>(this, distiller);
	    case PREPARE:
		nextResponse = ProtosForTest.PrepareResponseChecker.builder().build();
		return new FutureResponseMock<V>(this, distiller);
	    case DISPOSE_PREPARED_STATEMENT:
		nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
		return new FutureResponseMock<V>(this, distiller);
	    case DISCONNECT:
		nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
		return new FutureResponseMock<V>(this, distiller);
	    case EXPLAIN:
		nextResponse = ProtosForTest.ExplainResponseChecker.builder().build();
		return new FutureResponseMock<V>(this, distiller);
	    default:
		return null;  // dummy as it is test for session
	    }
	}

	public Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> sendQuery(RequestProtos.Request.Builder request) throws IOException {
	    return null;  // dummy as it is test for session
	}

	public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
	    var r = nextResponse;
	    nextResponse = null;
	    return r;
	}

	public ResultSetWire createResultSetWire() throws IOException {
	    return null;  // dummy as it is test for session
	}

	public void close() throws IOException {
	}
    }

    @Test
    void explain() {
	SessionLinkImpl sessionLink = new SessionLinkImpl(new SessionWireMock());

        try {
	    var r = sessionLink.send(ProtosForTest.ExplainChecker.builder());
	    assertEquals(ProtosForTest.ResMessageExplainChecker.builder().build().getOutput(), r.get());
	} catch (IOException | InterruptedException | ExecutionException e) {
            fail("cought Exception");
        }
   }
}