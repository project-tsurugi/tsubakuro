package com.tsurugidb.tsubakuro.sql.impl;

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

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.session.ProtosForTest;

class SessionImplTest {
    SqlResponse.Response nextResponse;
    private final long specialTimeoutValue = 9999;

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
        public FutureResponse<? extends Response> send(int serviceID, ByteBuffer request) throws IOException {
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
