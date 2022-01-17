package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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

class SessionImplTest {
    ResponseProtos.Response nextResponse;
    private final long specialTimeoutValue = 9999;

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
	public V get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	    if (timeout == specialTimeoutValue) {
		throw new TimeoutException("timeout for test");
	    }
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
	    case ROLLBACK:
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

    @Test
    void sessionTimeout() {
        SessionImpl session;

	session = new SessionImpl();
	session.connect(new SessionWireMock());
	session.setCloseTimeout(specialTimeoutValue, TimeUnit.SECONDS);

	Throwable exception = assertThrows(IOException.class, () -> {
		session.close();
	    });
	assertEquals("java.util.concurrent.TimeoutException: timeout for test", exception.getMessage());
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
    void useTransactionAfterSessionClose() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireMock());
	    var transaction = session.createTransaction().get();
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
    void usePreparedStatementAfterClose() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireMock());

	    String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
	    var ph = RequestProtos.PlaceHolder.newBuilder()
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8));
	    var preparedStatement = session.prepare(sql, ph).get();
	    preparedStatement.close();

	    var transaction = session.createTransaction().get();
	    var ps = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(99999999));

	    Throwable exception = assertThrows(IOException.class, () -> {
		    var pair = transaction.executeQuery(preparedStatement, ps);
		    var resultSet = pair.getLeft().get();
		});
	    assertEquals("already closed", exception.getMessage());

	    transaction.commit();
	    session.close();
	} catch (IOException e) {
            fail("cought IOException");
	} catch (InterruptedException e) {
            fail("cought InterruptedException");
	} catch (ExecutionException e) {
            fail("cought ExecutionException");
        }
   }

    @Test
    void usePreparedStatementAfterSessionClose() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireMock());

	    String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
	    var ph = RequestProtos.PlaceHolder.newBuilder()
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8));
	    var preparedStatement = session.prepare(sql, ph).get();
	    session.close();

	    Throwable exception = assertThrows(IOException.class, () -> {
		    var handle = ((PreparedStatementImpl) preparedStatement).getHandle();
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
}
