package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.StatusProtos;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.exception.ServerException;

class TransactionExceptionTest {
    ResponseProtos.Response nextResponse;
    ResponseProtos.Response processedResponse;
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

    class FutureQueryResponseMock implements FutureResponse<ResponseProtos.ExecuteQuery> {
        private final SessionWireMock wire;
        private ResponseWireHandle handle; // dummey
        FutureQueryResponseMock(SessionWireMock wire) {
            this.wire = wire;
        }

        @Override
        public ResponseProtos.ExecuteQuery get() throws IOException, ServerException {
            var response = wire.receive(handle);
            if (Objects.isNull(response)) {
                throw new IOException("received null response at FutureResponseMock, probably test program is incomplete");
            }
            if (ResponseProtos.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
                return response.getExecuteQuery();
            }
            wire.unReceive(handle);
            return null;
        }
        @Override
        public ResponseProtos.ExecuteQuery get(long timeout, TimeUnit unit) throws TimeoutException, IOException, ServerException {
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
        public <V> FutureResponse<V> send(long serviceID, RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
            switch (request.getRequestCase()) {
            case BEGIN:
                nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case PREPARE:
                nextResponse = ProtosForTest.PrepareResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case EXECUTE_STATEMENT:
            case EXECUTE_PREPARED_STATEMENT:
                nextResponse = ResponseProtos.Response.newBuilder()
                    .setResultOnly(ResponseProtos.ResultOnly.newBuilder()
                                   .setError(ResponseProtos.Error.newBuilder()
                                             .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                             .setDetail(messageForTheTest)))
                    .build();
                return new FutureResponseMock<>(this, distiller);
            case DISPOSE_PREPARED_STATEMENT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<>(this, distiller);
            case COMMIT:
                nextResponse = ResponseProtos.Response.newBuilder()
                    .setResultOnly(ResponseProtos.ResultOnly.newBuilder()
                                   .setError(ResponseProtos.Error.newBuilder()
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
                nextResponse = ResponseProtos.Response.newBuilder()
                    .setExplain(ResponseProtos.Explain.newBuilder()
                                   .setError(ResponseProtos.Error.newBuilder()
                                             .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                             .setDetail(messageForTheTest)))
                    .build();
                return new FutureResponseMock<>(this, distiller);
            default:
                return null;  // dummy as it is test for session
            }
        }

        @Override
        public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> sendQuery(long serviceID, RequestProtos.Request.Builder request) throws IOException {
            switch (request.getRequestCase()) {
            case EXECUTE_QUERY:
            case EXECUTE_PREPARED_QUERY:
                nextResponse = ResponseProtos.Response.newBuilder()
                    .setResultOnly(ResponseProtos.ResultOnly.newBuilder()
                                   .setError(ResponseProtos.Error.newBuilder()
                                             .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                             .setDetail(messageForTheTest)))
                    .build();
                var left = new FutureQueryResponseMock(this);
                var right = new FutureResponseMock<ResponseProtos.ResultOnly>(this, new ResultOnlyDistiller());
                return Pair.of(left, right);
            }
            return Pair.of(null, null);
        }

        @Override
        public ResponseProtos.Response receive(ResponseWireHandle handle) {
            processedResponse = nextResponse;
            nextResponse = null;
            return processedResponse;
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }

        @Override
        public ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
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
        var transaction = session.createTransaction().get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                transaction.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')").get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        session.close();
    }

    @Test
    void executePreparedStatementError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var transaction = session.createTransaction().get();

        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')";
        var ph = RequestProtos.PlaceHolder.newBuilder().build();
        var preparedStatement = session.prepare(sql, ph).get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                transaction.executeStatement(preparedStatement).get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        session.close();
    }

    @Test
    void executeQueryError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var transaction = session.createTransaction().get();
        var resultSet = transaction.executeQuery("SELECT * FROM ORDERS WHERE o_id = 1").get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                resultSet.getResponse().get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        session.close();
    }

    @Test
    void executePreparedQueryError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());

        String sql = "SELECT * FROM ORDERS WHERE o_id = 1";
        var ph = RequestProtos.PlaceHolder.newBuilder().build();
        var preparedStatement = session.prepare(sql, ph).get();

        var transaction = session.createTransaction().get();
        var resultSet = transaction.executeQuery(preparedStatement).get();
        Throwable exception = assertThrows(ServerException.class, () -> {
                resultSet.getResponse().get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        session.close();
    }

    @Test
    void commitError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var transaction = session.createTransaction().get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                transaction.commit().get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        session.close();
    }

    @Test
    void explainError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());

        String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
        var ph = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8)).build();
        var preparedStatement = session.prepare(sql, ph).get();

        Throwable exception = assertThrows(ServerException.class, () -> {
                session.explain(preparedStatement, RequestProtos.ParameterSet.newBuilder().build()).get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        session.close();
    }
}
