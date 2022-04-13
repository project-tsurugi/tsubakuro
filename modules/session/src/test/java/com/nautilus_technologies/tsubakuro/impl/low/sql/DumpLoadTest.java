package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.nio.file.Path;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;
    
import org.junit.jupiter.api.Test;

class DumpLoadTest {
    ResponseProtos.Response nextResponse;

    private class PreparedStatementMock implements PreparedStatement {
	PreparedStatementMock() {
	}
	public CommonProtos.PreparedStatement getHandle() throws IOException {
	    return null;
	}
	public void setCloseTimeout(long t, TimeUnit u) {
	}

	public void close() throws IOException {
        }
    }

    class FutureResponseTestMock<V> implements Future<V> {
	private SessionWireTestMock wire;
	private Distiller<V> distiller;
	private ResponseWireHandle handle; // dummey
	FutureResponseTestMock(SessionWireTestMock wire, Distiller<V> distiller) {
	    this.wire = wire;
	    this.distiller = distiller;
	}
        public V get() throws ExecutionException {
	    try {
		var response = wire.receive(handle);
		if (Objects.isNull(response)) {
		    throw new IOException("received null response at FutureResponseTestMock, probably test program is incomplete");
		}
		return distiller.distill(response);
	    } catch (IOException e) {
		throw new ExecutionException(e);
	    }
	}
	public V get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
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

    class SessionWireTestMock implements SessionWire {
	public <V> Future<V> send(RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
	    switch (request.getRequestCase()) {
	    case BEGIN:
		nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
		return new FutureResponseTestMock<V>(this, distiller);
	    case DISCONNECT:
		nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
		return new FutureResponseTestMock<V>(this, distiller);
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
	public ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
	    return null;  // dummy as it is test for session
	}
	public void unReceive(ResponseWireHandle responseWireHandle) {
	}
	public void close() throws IOException {
	}
    }
    
    @Test
    void loadOK() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireTestMock());

	    var preparedStatement = new PreparedStatementMock();

	    var opts = RequestProtos.TransactionOption.newBuilder()
		.setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_LONG)
		.addWritePreserves(RequestProtos.TransactionOption.WritePreserve.newBuilder().setName("LOAD_TARGET"))
		.build();
	    
	    Future<Transaction> fTransaction = session.createTransaction(opts);

	    try (Transaction transaction = fTransaction.get()) {
		List<Path> paths = new ArrayList<>();
		paths.add(Path.of("/load_directory/somefile"));
		var response = transaction.executeLoad(preparedStatement, 
						       RequestProtos.ParameterSet.newBuilder().build(),
						       paths).get();

		assertTrue(ProtosForTest.ResultOnlyChecker.check(response));

		transaction.commit();
		session.close();
	    } catch (IOException | InterruptedException | ExecutionException e) {
		throw e;
	    }
	} catch (IOException e) {
            fail("cought IOException");
	} catch (InterruptedException e) {
            fail("cought InterruptedException");
	} catch (ExecutionException e) {
            fail("cought ExecutionException");
        }
    }

    @Test
    void loadNG() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireTestMock());

	    var preparedStatement = new PreparedStatementMock();

	    var opts = RequestProtos.TransactionOption.newBuilder()
		.setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_LONG)
		.addWritePreserves(RequestProtos.TransactionOption.WritePreserve.newBuilder().setName("LOAD_TARGET"))
		.build();
	    
	    Future<Transaction> fTransaction = session.createTransaction(opts);

	    try (Transaction transaction = fTransaction.get()) {
		List<Path> paths = new ArrayList<>();
		paths.add(Path.of("/load_directory/NGfile"));  // when file name includes "NG", executeLoad() will return error.
		var response = transaction.executeLoad(preparedStatement, 
						       RequestProtos.ParameterSet.newBuilder().build(),
						       paths).get();

		assertFalse(ProtosForTest.ResultOnlyChecker.check(response));

		transaction.commit();
		session.close();
	    } catch (IOException | InterruptedException | ExecutionException e) {
		throw e;
	    }
	} catch (IOException e) {
            fail("cought IOException");
	} catch (InterruptedException e) {
            fail("cought InterruptedException");
	} catch (ExecutionException e) {
            fail("cought ExecutionException");
        }
    }

    @Test
    void dumpOK() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireTestMock());

	    var preparedStatement = new PreparedStatementMock();
	    var target = Path.of("/dump_directory");

	    var opts = RequestProtos.TransactionOption.newBuilder()
		.setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_LONG)
		.addWritePreserves(RequestProtos.TransactionOption.WritePreserve.newBuilder().setName("LOAD_TARGET"))
		.build();

	    Future<Transaction> fTransaction = session.createTransaction(opts);

	    try (Transaction transaction = fTransaction.get()) {
		Future<ResultSet> fResults = transaction.executeDump(preparedStatement,
						   RequestProtos.ParameterSet.newBuilder().build(),
						   target);

		var results = fResults.get();
		assertTrue(Objects.nonNull(results));

		int recordCount = 0;
		int columnCount = 0;
		while (results.nextRecord()) {
		    while (results.nextColumn()) {
			    assertEquals(results.type(), CommonProtos.DataType.CHARACTER);
			    assertEquals(results.getCharacter(), ResultSetMock.FILE_NAME);
			    columnCount++;
		    }
		    recordCount++;
		}
		assertEquals(columnCount, 1);
		assertEquals(recordCount, 1);

		assertTrue(ProtosForTest.ResultOnlyChecker.check(results.getFutureResponse().get()));

		transaction.commit();
		session.close();
	    } catch (IOException | InterruptedException | ExecutionException e) {
		throw e;
	    }
	} catch (IOException e) {
            fail("cought IOException");
	} catch (InterruptedException e) {
            fail("cought InterruptedException");
	} catch (ExecutionException e) {
            fail("cought ExecutionException");
        }
    }

    @Test
    void dumpNG() {
	SessionImpl session;
        try {
	    session = new SessionImpl();
	    session.connect(new SessionWireTestMock());

	    var preparedStatement = new PreparedStatementMock();
	    var target = Path.of("/dump_NGdirectory");  // when directory name includes "NG", executeDump() will return error.

	    var opts = RequestProtos.TransactionOption.newBuilder()
		.setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_LONG)
		.addWritePreserves(RequestProtos.TransactionOption.WritePreserve.newBuilder().setName("LOAD_TARGET"))
		.build();

	    Future<Transaction> fTransaction = session.createTransaction(opts);

	    try (Transaction transaction = fTransaction.get()) {
		Future<ResultSet> fResults = transaction.executeDump(preparedStatement,
						   RequestProtos.ParameterSet.newBuilder().build(),
						   target);
		var results = fResults.get();
		assertTrue(Objects.nonNull(results));
		assertFalse(ProtosForTest.ResultOnlyChecker.check(results.getFutureResponse().get()));

		transaction.commit();
		session.close();
	    } catch (IOException | InterruptedException | ExecutionException e) {
		throw e;
	    }
	} catch (IOException e) {
            fail("cought IOException");
	} catch (InterruptedException e) {
            fail("cought InterruptedException");
	} catch (ExecutionException e) {
            fail("cought ExecutionException");
        }
    }
}
