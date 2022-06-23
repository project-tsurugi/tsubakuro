package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.low.sql.Types;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.Owner;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;

class SessionImplTest {
    SqlResponse.Response nextResponse;
    private final long specialTimeoutValue = 9999;

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
        public ResponseWireHandle responseWireHandle() {
            return handle;
        }
        @Override
        public void release() {
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
                case DISPOSE_PREPARED_STATEMENT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case ROLLBACK:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case DISCONNECT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case EXPLAIN:
                    nextResponse = ProtosForTest.ExplainResponseChecker.builder().build();
                    break;
                case DESCRIBE_TABLE:
                    nextResponse =  SqlResponse.Response.newBuilder()
                    .setDescribeTable(SqlResponse.DescribeTable.newBuilder()
                        .setSuccess(SqlResponse.DescribeTable.Success.newBuilder()
                             .setDatabaseName("D")
                             .setSchemaName("S")
                             .setTableName(request.getDescribeTable().getName())
                             .addColumns(Types.column("a", Types.of(int.class)))
                        )
                    ).build();
                    break;
                default:
                    return null;  // dummy as it is test for session
            }
            return FutureResponse.wrap(Owner.of(new ChannelResponseMock(this)));
        }

        @Override
        public FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) throws IOException {
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
        public void setQueryMode(ResponseWireHandle responseWireHandle) {
        }

        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
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

    @Test
    void getTableMetadata() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);

        var info = sqlClient.getTableMetadata("TBL").await();
        assertEquals(Optional.of("D"), info.getDatabaseName());
        assertEquals(Optional.of("S"), info.getSchemaName());
        assertEquals("TBL", info.getTableName());
    }
}
