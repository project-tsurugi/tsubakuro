package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.Collection;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.tsurugidb.jogasaki.proto.StatusProtos;
import com.nautilus_technologies.tsubakuro.util.Owner;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;

class TransactionExceptionTest {
    SqlResponse.Response nextResponse;
    SqlResponse.Response processedResponse;
    private final long specialTimeoutValue = 9999;
    private final String messageForTheTest = "this is a error message for the test";

    class ResponseWireHandleDummy extends ResponseWireHandle {
        ResponseWireHandleDummy() {
        }
    }

    class ChannelResponseMock implements Response {
        private final SessionWireMock wire;
        private ResponseWireHandle handle;

        ChannelResponseMock(SessionWireMock wire) {
            this.wire = wire;
            this.handle = new ResponseWireHandleDummy();
        }
        @Override
        public boolean isMainResponseReady() {
            return Objects.nonNull(handle);
        }
        @Override
        public ByteBuffer waitForMainResponse() throws IOException {
            return wire.response(handle);
        }
        @Override
        public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException {
            return wire.response(handle);
        }
        @Override
        public Collection<String> getSubResponseIds() throws IOException, ServerException, InterruptedException {
            return null;
        }
        @Override
        public InputStream openSubResponse(String id) throws IOException, ServerException, InterruptedException {
            return null;
        }
        @Override
        public void close() throws IOException, InterruptedException {
        }
        @Override
        public ResponseWireHandle responseWireHandle() {
            return handle;
        }
        @Override
        public void setQueryMode() {
        }
    }

    class SessionWireMock implements SessionWire {

        @Override
        public FutureResponse<? extends Response> send(long serviceID, byte[] byteArray) throws IOException {
            var request = SqlRequest.Request.parseDelimitedFrom(new ByteArrayInputStream(byteArray));
            switch (request.getRequestCase()) {
                case BEGIN:
                    nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
                    break;
                case PREPARE:
                    nextResponse = ProtosForTest.PrepareResponseChecker.builder().build();
                    break;
                case EXECUTE_STATEMENT:
                case EXECUTE_PREPARED_STATEMENT:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                       .setError(SqlResponse.Error.newBuilder()
                                                 .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                                 .setDetail(messageForTheTest)))
                        .build();
                        break;
                case EXECUTE_QUERY:
                case EXECUTE_PREPARED_QUERY:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                       .setError(SqlResponse.Error.newBuilder()
                                                 .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                                 .setDetail(messageForTheTest)))
                        .build();
                        break;
                case DISPOSE_PREPARED_STATEMENT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case COMMIT:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                       .setError(SqlResponse.Error.newBuilder()
                                                 .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                                 .setDetail(messageForTheTest)))
                        .build();
                        break;
                case ROLLBACK:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case DISCONNECT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case EXPLAIN:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setExplain(SqlResponse.Explain.newBuilder()
                                       .setError(SqlResponse.Error.newBuilder()
                                                 .setStatus(StatusProtos.Status.ERR_UNKNOWN)
                                                 .setDetail(messageForTheTest)))
                        .build();
                        break;
                default:
                    return null;  // dummy as it is test for session
            }
            return FutureResponse.wrap(Owner.of(new ChannelResponseMock(this)));
        }

        @Override
        public FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) {
            return null; // dummy as it is test for session
        }

        @Override
        public ByteBuffer response(ResponseWireHandle handle) throws IOException {
            processedResponse = nextResponse;  // FIXME should do clone()
            return ByteBuffer.wrap(DelimitedConverter.toByteArray(processedResponse));
        }

        @Override
        public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws IOException {
            return response(handle); // dummy as it is test for session
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }
        @Override
        public void setQueryMode(ResponseWireHandle responseWireHandle) {
        }
        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
            nextResponse = processedResponse;
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

        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (123, 456.789, 'abcdef')";
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
