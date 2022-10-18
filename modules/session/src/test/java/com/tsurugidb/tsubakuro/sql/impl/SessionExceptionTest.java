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
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.sql.proto.SqlStatus;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.session.ProtosForTest;

class SesstionExceptionTest {
    SqlResponse.Response nextResponse;
    private final long specialTimeoutValue = 9999;
    private final String messageForTheTest = "this is a error message for the test";

    class ChannelResponseMock implements Response {
        private final SessionWireMock wire;

        ChannelResponseMock(SessionWireMock wire) {
            this.wire = wire;
        }
        @Override
        public boolean isMainResponseReady() {
            return true;
        }
        @Override
        public ByteBuffer waitForMainResponse() throws IOException {
            return ByteBuffer.wrap(DelimitedConverter.toByteArray(nextResponse));
        }
        @Override
        public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException {
            return waitForMainResponse();
        }
        @Override
        public void close() throws IOException, InterruptedException {
        }
    }

    class SessionWireMock implements Wire {
        @Override
        public FutureResponse<? extends Response> send(int serviceID, byte[] byteArray) throws IOException {
            var request = SqlRequest.Request.parseDelimitedFrom(new ByteArrayInputStream(byteArray));
            switch (request.getRequestCase()) {
                case BEGIN:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setBegin(SqlResponse.Begin.newBuilder()
                                  .setError(SqlResponse.Error.newBuilder()
                                            .setStatus(SqlStatus.Status.ERR_UNKNOWN)
                                            .setDetail(messageForTheTest)))
                        .build();
                        break;
                case PREPARE:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setPrepare(SqlResponse.Prepare.newBuilder()
                                  .setError(SqlResponse.Error.newBuilder()
                                            .setStatus(SqlStatus.Status.ERR_UNKNOWN)
                                            .setDetail(messageForTheTest)))
                        .build();
                        break;
                case DISPOSE_PREPARED_STATEMENT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case ROLLBACK:
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
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }

        @Override
        public void close() throws IOException {
        }
    }

    @Test
    void beginError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        Throwable exception = assertThrows(ServerException.class, () -> {
                sqlClient.createTransaction().get();
        });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }

    @Test
    void prepareError() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        Throwable exception = assertThrows(ServerException.class, () -> {
                String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
                sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
            });
        // FIXME: check structured error code instead of message
        assertTrue(exception.getMessage().contains(messageForTheTest));

        sqlClient.close();
    }
}
