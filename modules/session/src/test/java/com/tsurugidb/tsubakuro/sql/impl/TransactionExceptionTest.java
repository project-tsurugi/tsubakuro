package com.tsurugidb.tsubakuro.sql.impl;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseWireHandle;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.sql.proto.SqlStatus;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.session.ProtosForTest;

class TransactionExceptionTest {
    SqlResponse.Response nextResponse;
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
        public void close() throws IOException, InterruptedException {
        }
        @Override
        public ChannelResponseMock duplicate() {
            var rv = new ChannelResponseMock(wire);
            return rv;
        }
        @Override
        public void release() {
        }
        @Override
        public void setResultSetMode() {
        }
    }

    class SessionWireMock implements Wire {

        @Override
        public FutureResponse<? extends Response> send(int serviceID, byte[] byteArray) throws IOException {
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
                                                 .setStatus(SqlStatus.Status.ERR_UNKNOWN)
                                                 .setDetail(messageForTheTest)))
                        .build();
                        break;
                case EXECUTE_QUERY:
                case EXECUTE_PREPARED_QUERY:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                       .setError(SqlResponse.Error.newBuilder()
                                                 .setStatus(SqlStatus.Status.ERR_UNKNOWN)
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
                                                 .setStatus(SqlStatus.Status.ERR_UNKNOWN)
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
                                                 .setStatus(SqlStatus.Status.ERR_UNKNOWN)
                                                 .setDetail(messageForTheTest)))
                        .build();
                        break;
                default:
                    return null;  // dummy as it is test for session
            }
            return FutureResponse.wrap(Owner.of(new ChannelResponseMock(this)));
        }

        @Override
        public FutureResponse<? extends Response> send(int serviceID, ByteBuffer request) {
            return null; // dummy as it is test for session
        }

        @Override
        public ByteBuffer response(ResponseWireHandle handle) throws IOException {
            return ByteBuffer.wrap(DelimitedConverter.toByteArray(nextResponse));
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
        public void release(ResponseWireHandle responseWireHandle) {
        }
        @Override
        public void setResultSetMode(ResponseWireHandle responseWireHandle) {
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
        var ph = SqlRequest.Placeholder.newBuilder().build();
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
        var futureResultSet = transaction.executeQuery("SELECT * FROM ORDERS WHERE o_id = 1");

        Throwable exception = assertThrows(ServerException.class, () -> {
                futureResultSet.get();
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
        var ph = SqlRequest.Placeholder.newBuilder().build();
        var preparedStatement = sqlClient.prepare(sql, ph).get();

        var transaction = sqlClient.createTransaction().get();
        var futureResultSet = transaction.executeQuery(preparedStatement);

        Throwable exception = assertThrows(ServerException.class, () -> {
                futureResultSet.get();
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
