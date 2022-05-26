package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.tsurugidb.jogasaki.proto.StatusProtos;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;

class TransactionExceptionTest {
    SqlResponse.Response nextResponse;
    SqlResponse.Response processedResponse;
    private final long specialTimeoutValue = 9999;
    private final String messageForTheTest = "this is a error message for the test";

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

    class FutureQueryResponseMock implements FutureResponse<SqlResponse.ExecuteQuery> {
        private final SessionWireMock wire;
        private ResponseWireHandle handle; // dummey
        FutureQueryResponseMock(SessionWireMock wire) {
            this.wire = wire;
        }

        @Override
        public SqlResponse.ExecuteQuery get() throws IOException, ServerException {
            var response = wire.receive(handle);
            if (Objects.isNull(response)) {
                throw new IOException("received null response at FutureResponseMock, probably test program is incomplete");
            }
            if (SqlResponse.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
                return response.getExecuteQuery();
            }
            wire.unReceive(handle);
            return null;
        }
        @Override
        public SqlResponse.ExecuteQuery get(long timeout, TimeUnit unit) throws TimeoutException, IOException, ServerException {
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
            case EXECUTE_STATEMENT:
            case EXECUTE_PREPARED_STATEMENT:
                nextResponse = SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                   .setError(SqlResponse.Error.newBuilder()
                                             .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                             .setDetail(messageForTheTest)))
                    .build();
                return new FutureResponseMock<>(this, distiller);
            case DISPOSE_PREPARED_STATEMENT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case COMMIT:
                nextResponse = SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                   .setError(SqlResponse.Error.newBuilder()
                                             .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                             .setDetail(messageForTheTest)))
                    .build();
                return new FutureResponseMock<>(this, distiller);
            case ROLLBACK:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case DISCONNECT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case EXPLAIN:
                nextResponse = SqlResponse.Response.newBuilder()
                    .setExplain(SqlResponse.Explain.newBuilder()
                                   .setError(SqlResponse.Error.newBuilder()
                                             .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                             .setDetail(messageForTheTest)))
                    .build();
                return new FutureResponseMock<>(this, distiller);
            default:
                return null;  // dummy as it is test for session
            }
        }

        @Override
        public Pair<FutureResponse<SqlResponse.ExecuteQuery>, FutureResponse<SqlResponse.ResultOnly>> sendQuery(long serviceID, SqlRequest.Request.Builder request) throws IOException {
            switch (request.getRequestCase()) {
            case EXECUTE_QUERY:
            case EXECUTE_PREPARED_QUERY:
                nextResponse = SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                   .setError(SqlResponse.Error.newBuilder()
                                             .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                             .setDetail(messageForTheTest)))
                    .build();
                var left = new FutureQueryResponseMock(this);
                var right = new FutureResponseMock<SqlResponse.ResultOnly>(this, new ResultOnlyDistiller());
                return Pair.of(left, right);
            default:
                return Pair.of(null, null);
            }
        }

        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle) {
            processedResponse = nextResponse;
            nextResponse = null;
            return processedResponse;
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }

        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            return receive(handle);  // do not handle timeout as it is a test
        }

        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
            nextResponse = processedResponse;
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

    @Test
    void executeStatementError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        var transaction = sqlClient.createTransaction().get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                transaction.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')").get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }

    @Test
    void executePreparedStatementError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        var transaction = sqlClient.createTransaction().get();

        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')";
        var ph = SqlRequest.PlaceHolder.newBuilder().build();
        var preparedStatement = sqlClient.prepare(sql, ph).get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                transaction.executeStatement(preparedStatement).get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }

    @Test
    void executeQueryError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        var transaction = sqlClient.createTransaction().get();
        var resultSet = transaction.executeQuery("SELECT * FROM ORDERS WHERE o_id = 1").get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                resultSet.getResponse().get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }

    @Test
    void executePreparedQueryError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        String sql = "SELECT * FROM ORDERS WHERE o_id = 1";
        var ph = SqlRequest.PlaceHolder.newBuilder().build();
        var preparedStatement = sqlClient.prepare(sql, ph).get();

        var transaction = sqlClient.createTransaction().get();
        var resultSet = transaction.executeQuery(preparedStatement).get();
        Throwable exception = assertThrows(ServerException.class, () -> {
                resultSet.getResponse().get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }

    @Test
    void commitError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        var transaction = sqlClient.createTransaction().get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                transaction.commit().get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }

    @Test
    void explainError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
        var preparedStatement = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                sqlClient.explain(preparedStatement, Parameters.of("o_id", 99999999L)).get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }
}
