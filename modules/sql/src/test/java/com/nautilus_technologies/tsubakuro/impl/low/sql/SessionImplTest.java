package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Future;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;
    
import org.junit.jupiter.api.Test;

class SessionImplTest {
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
		return distiller.distill(wire.receive(handle));
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
	    if (RequestProtos.Request.RequestCase.BEGIN.equals(request.getRequestCase())) {
		nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
		return new FutureResponseMock<V>(this, distiller);
	    }
	    return null;  // dummy as it is test for session
	}

	public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
	    var r = nextResponse;
	    nextResponse = null;
	    return r;
	}

	public ResultSetWire createResultSetWire(String name) throws IOException {
	    return null;  // dummy as it is test for session
	}

	public void close() throws IOException {
	}
    }

    @Test
    void useTransactionAfterClose() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireMock());
	    var transaction = session.createTransaction().get();
	    transaction.commit();
	    session.close();

	    Throwable exception = assertThrows(IOException.class, () -> {
		    transaction.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')");
		});
	    assertEquals("already closed", exception.getMessage());
	} catch (IOException e) {
            fail("cought IOException");
	} catch (InterruptedException e) {
            fail("cought InterruptedException");
	} catch (ExecutionException e) {
            fail("cought ExecutionException");
        }
    }

    @Test
    void useSessionAfterClose() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireMock());
	    session.close();

	    Throwable exception = assertThrows(IOException.class, () -> {
		    session.createTransaction();
		});
	    assertEquals("this session is not connected to the Database", exception.getMessage());
	} catch (IOException e) {
            fail("cought IOException");
        }
   }
}
