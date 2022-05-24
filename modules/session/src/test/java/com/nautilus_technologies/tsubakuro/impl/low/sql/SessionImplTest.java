package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.FutureInputStream;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.tsurugidb.jogasaki.proto.Distiller;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;

class SessionImplTest {
    SqlResponse.Response nextResponse;
    private final long specialTimeoutValue = 9999;

    class FutureResponseMock<V> implements FutureResponse<V> {
        private final SessionWireMock wire;
        private final Distiller<V> distiller;
        private ResponseWireHandle handle; // dummey
        FutureResponseMock(SessionWireMock wire, Distiller<V> distiller) {
            this.wire = wire;
            this.distiller = distiller;
        }

        @Override
        public V get() throws IOException, ServerException {
            var response = wire.receive(handle);
            if (Objects.isNull(response)) {
                throw new IOException("received null response at FutureResponseMock, probably test program is incomplete");
            }
            return distiller.distill(response);
        }
        @Override
        public V get(long timeout, TimeUnit unit) throws TimeoutException, IOException, ServerException {
            if (timeout == specialTimeoutValue) {
                throw new TimeoutException("timeout for test");
            }
            return get();  // FIXME need to be implemented properly, same as below
        }
        @Override
        public boolean isDone() {
            return true;
        }
        @Override
        public void close() throws IOException, ServerException, InterruptedException {
        }
    }

    class SessionWireMock implements SessionWire {
        @Override
        public <V> FutureResponse<V> send(long serviceID, SqlRequest.Request.Builder request, Distiller<V> distiller) throws IOException {
            switch (request.getRequestCase()) {
            case BEGIN:
                nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case PREPARE:
                nextResponse = ProtosForTest.PrepareResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case DISPOSE_PREPARED_STATEMENT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case ROLLBACK:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case DISCONNECT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case EXPLAIN:
                nextResponse = ProtosForTest.ExplainResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            default:
                return null;  // dummy as it is test for session
            }
        }

        @Override
        public Pair<FutureResponse<SqlResponse.ExecuteQuery>, FutureResponse<SqlResponse.ResultOnly>> sendQuery(long serviceID, SqlRequest.Request.Builder request) throws IOException {
            return null;  // dummy as it is test for session
        }

        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle) throws IOException {
            var r = nextResponse;
            nextResponse = null;
            return r;
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }

        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            var r = nextResponse;
            nextResponse = null;
            return r;
        }

        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
        }

        @Override
        public FutureInputStream send(long serviceID, byte[] request) {
            return null; // dummy as it is test for session
        }

        @Override
        public InputStream responseStream(ResponseWireHandle handle) {
            return null; // dummy as it is test for session
        }

        @Override
        public InputStream responseStream(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            return null; // dummy as it is test for session
        }

        
        @Override
        public void close() throws IOException {
        }
    }

    
    @Disabled("not implemented")  // FIXME implement close handling of Session
    @Test
    void useSessionAfterClose() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        sqlClient.close();

        Throwable exception = assertThrows(IOException.class, () -> {
                sqlClient.createTransaction();
        });
        // FIXME: check structured error code instead of message
        assertEquals("this session is not connected to the Database", exception.getMessage());
    }

    @Disabled("timeout should raise whether error or warning")
    @Test
    void sessionTimeout() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        session.setCloseTimeout(specialTimeoutValue, TimeUnit.SECONDS);

        Throwable exception = assertThrows(IOException.class, () -> {
            session.close();
        });
        // FIXME: check structured error code instead of message
        assertEquals("java.util.concurrent.TimeoutException: timeout for test", exception.getMessage());
    }

    @Disabled("not implemented")  // FIXME implement close handling of Transaction
    @Test
    void useTransactionAfterClose() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        var transaction = sqlClient.createTransaction().get();
        transaction.commit();

        Throwable exception = assertThrows(IOException.class, () -> {
            transaction.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')");
        });
        // FIXME: check structured error code instead of message
        assertEquals("already closed", exception.getMessage());
    }

    @Disabled("not implemented")  // FIXME implement close handling of Session
    @Test
    void useTransactionAfterSessionClose() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        var t1 = sqlClient.createTransaction().get();
        var t2 = sqlClient.createTransaction().get();
        var t3 = sqlClient.createTransaction().get();
        var t4 = sqlClient.createTransaction().get();

        t2.commit();
        t4.commit();

        sqlClient.close();

        Throwable e1 = assertThrows(IOException.class, () -> {
            t1.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')");
        });
        // FIXME: check structured error code instead of message
        assertEquals("already closed", e1.getMessage());

        Throwable e2 = assertThrows(IOException.class, () -> {
            t2.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')");
        });
        assertEquals("already closed", e2.getMessage());
    }

    @Disabled("not implemented")  // FIXME implement close handling of PreparedStatement
    @Test
    void usePreparedStatementAfterClose() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        
        String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
        var preparedStatement = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        preparedStatement.close();
        
        var transaction = sqlClient.createTransaction().get();
        
        Throwable exception = assertThrows(IOException.class, () -> {
                var resultSet = transaction.executeQuery(preparedStatement, Parameters.of("o_id", 99999999L)).await();
            });
        // FIXME: check structured error code instead of message
        assertEquals("already closed", exception.getMessage());
        
        transaction.commit();
        sqlClient.close();
    }

    @Disabled("not implemented")  // FIXME implement close handling of PreparedStatement
    @Test
    void usePreparedStatementAfterSessionClose() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        
        String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
        var ps1 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        var ps2 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        var ps3 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        var ps4 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        
        ps2.close();
        ps4.close();
        sqlClient.close();
        
        Throwable e1 = assertThrows(IOException.class, () -> {
                var handle = ((PreparedStatementImpl) ps1).getHandle();
            });
        // FIXME: check structured error code instead of message
        assertEquals("already closed", e1.getMessage());
        Throwable e2 = assertThrows(IOException.class, () -> {
                var handle = ((PreparedStatementImpl) ps2).getHandle();
            });
        // FIXME: check structured error code instead of message
        assertEquals("already closed", e2.getMessage());
    }
}
