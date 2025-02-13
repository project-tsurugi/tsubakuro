/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.sql.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
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
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.session.ProtosForTest;

class SessionImplTest {
    SqlResponse.Response nextResponse;
    private final long specialTimeoutValue = 9999;
    private final String exceptionMessage = "link is already closed";

    class ChannelResponseMock implements Response {
        private final SessionWireMock wire;
        private boolean cancelCalled = false;

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
        public void cancel() throws IOException {
            cancelCalled = true;
        }
        @Override
        public void close() throws IOException, InterruptedException {
        }
        boolean cancelCalled() {
            return cancelCalled;
        }
    }

    class SessionWireMock implements Wire {
        private boolean closed = false;
        private ChannelResponseMock channelResponseMock;

        @Override
        public FutureResponse<? extends Response> send(int serviceID, byte[] byteArray) throws IOException {
            if (closed) {
                throw new IOException(exceptionMessage);
            }

            var request = SqlRequest.Request.parseDelimitedFrom(new ByteArrayInputStream(byteArray));
            switch (request.getRequestCase()) {
                case BEGIN:
                    nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
                    break;
                case PREPARE:
                    nextResponse = ProtosForTest.PrepareResponseChecker.builder().build();
                    break;
                case EXECUTE_STATEMENT:  // for cancel test
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setExecuteResult(SqlResponse.ExecuteResult.newBuilder())
                        .build();
                    break;
                case DISPOSE_PREPARED_STATEMENT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case COMMIT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case ROLLBACK:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                case DISPOSE_TRANSACTION:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setDisposeTransaction(SqlResponse.DisposeTransaction.newBuilder()
                                       .setSuccess(SqlResponse.Void.newBuilder()))
                        .build();
                        break;
                default:
                    System.out.println("falls default case%n" + request);
                    return null;  // dummy as it is test for session
            }
            channelResponseMock = new ChannelResponseMock(this);
            return FutureResponse.wrap(Owner.of(channelResponseMock));
        }

        @Override
        public FutureResponse<? extends Response> send(int serviceID, ByteBuffer request) throws IOException {
            return send(serviceID, request.array());
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            if (closed) {
                throw new IOException(exceptionMessage);
            }
            return null;  // dummy as it is test for session
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        ChannelResponseMock channelResponseMock() {
            return channelResponseMock;
        }
    }

    @Test
    void useSessionAfterClose() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        sqlClient.close();
        session.close();

        Throwable exception = assertThrows(IOException.class, () -> {
                sqlClient.createTransaction();
        });
        // FIXME: check structured error code instead of message
        assertEquals(exceptionMessage, exception.getMessage());
    }

    @Disabled("timeout should raise whether error or warning")
    @Test
    void sessionTimeout() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        session.setCloseTimeout(new Timeout(specialTimeoutValue, TimeUnit.SECONDS, Timeout.Policy.ERROR));

        Throwable exception = assertThrows(IOException.class, () -> {
                session.close();
            });
        // FIXME: check structured error code instead of message
        assertEquals("java.util.concurrent.TimeoutException: timeout for test", exception.getMessage());
    }

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
        assertEquals("transaction already closed", exception.getMessage());
    }

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
        assertEquals("transaction already closed", e1.getMessage());

        Throwable e2 = assertThrows(IOException.class, () -> {
                t2.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')");
            });
        assertEquals("transaction already closed", e2.getMessage());
    }

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
    void requestCancel() throws Exception {
        var session = new SessionImpl();
        var wire = new SessionWireMock();
        session.connect(wire);
        var sqlClient = SqlClient.attach(session);

        var transaction = sqlClient.createTransaction().get();
        var future = transaction.executeStatement("this is a sql for test");
        future.close();

        assertTrue(wire.channelResponseMock().cancelCalled());
    }
}
