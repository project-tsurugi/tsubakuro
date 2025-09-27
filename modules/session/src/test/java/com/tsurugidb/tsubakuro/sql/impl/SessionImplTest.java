/*
 * Copyright 2023-2025 Project Tsurugi.
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

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.session.ProtosForTest;

class SessionImplTest {

    private final MockLink link = new MockLink();

    private final long specialTimeoutValue = 9999;

    private final String exceptionMessage = "already closed";

    private static int SERVICE_ID_ENDPOINT_BROKER = 1;

    @Test
    void useSessionAfterClose() throws Exception {
        var wire = new WireImpl(link);
        var session = new SessionImpl();
        session.connect(wire);
        var sqlClient = SqlClient.attach(session);
        sqlClient.close();
        session.close();
        session.waitForCompletion();

        Throwable exception = assertThrows(IOException.class, () -> {
                sqlClient.createTransaction();
        });
        // FIXME: check structured error code instead of message
        assertEquals(exceptionMessage, exception.getMessage());
    }

    @Disabled("timeout should raise whether error or warning")
    @Test
    void sessionTimeout() throws Exception {
        var wire = new WireImpl(link);
        var session = new SessionImpl();
        session.connect(wire);
        session.setCloseTimeout(new Timeout(specialTimeoutValue, TimeUnit.SECONDS, Timeout.Policy.ERROR));

        Throwable exception = assertThrows(IOException.class, () -> {
                session.close();
            });
        // FIXME: check structured error code instead of message
        assertEquals("java.util.concurrent.TimeoutException: timeout for test", exception.getMessage());
    }

    @Test
    void useTransactionAfterClose() throws Exception {
        var wire = new WireImpl(link);
        var session = new SessionImpl();
        session.connect(wire);
        var sqlClient = SqlClient.attach(session);

        link.next(ProtosForTest.BeginResponseChecker.builder().build());
        var transaction = sqlClient.createTransaction().get();

        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        transaction.commit();

        Throwable exception = assertThrows(IOException.class, () -> {
                transaction.executeStatement("INSERT INTO tbl (c1, c2, c3) VALUES (123, 456,789, 'abcdef')");
            });
        // FIXME: check structured error code instead of message
        assertEquals("transaction already closed", exception.getMessage());
    }

    @Test
    void useTransactionAfterSessionClose() throws Exception {
        var wire = new WireImpl(link);
        var session = new SessionImpl();
        session.connect(wire);
        var sqlClient = SqlClient.attach(session);

        link.next(ProtosForTest.BeginResponseChecker.builder().build());
        link.next(ProtosForTest.BeginResponseChecker.builder().build());
        link.next(ProtosForTest.BeginResponseChecker.builder().build());
        link.next(ProtosForTest.BeginResponseChecker.builder().build());
        var t1 = sqlClient.createTransaction().get();
        var t2 = sqlClient.createTransaction().get();
        var t3 = sqlClient.createTransaction().get();
        var t4 = sqlClient.createTransaction().get();

        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        t2.commit();
        t4.commit();

        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        sqlClient.close();
        session.close();
        session.waitForCompletion();

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
        var wire = new WireImpl(link);
        var session = new SessionImpl();
        session.connect(wire);
        var sqlClient = SqlClient.attach(session);

        link.next(ProtosForTest.PrepareResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
        var preparedStatement = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        preparedStatement.close();
        session.waitForDisposerEmpty();

        link.next(ProtosForTest.BeginResponseChecker.builder().build());
        var transaction = sqlClient.createTransaction().get();

        Throwable exception = assertThrows(IOException.class, () -> {
                var resultSet = transaction.executeQuery(preparedStatement, Parameters.of("o_id", 99999999L)).await();
            });
        // FIXME: check structured error code instead of message
        assertEquals("already closed", exception.getMessage());

        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        transaction.commit();
        sqlClient.close();
    }

    @Test
    void usePreparedStatementAfterSessionClose() throws Exception {
        var wire = new WireImpl(link);
        var session = new SessionImpl();
        session.connect(wire);
        var sqlClient = SqlClient.attach(session);

        link.next(ProtosForTest.PrepareResponseChecker.builder().build());
        link.next(ProtosForTest.PrepareResponseChecker.builder().build());
        link.next(ProtosForTest.PrepareResponseChecker.builder().build());
        link.next(ProtosForTest.PrepareResponseChecker.builder().build());
        String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
        var ps1 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        var ps2 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        var ps3 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();
        var ps4 = sqlClient.prepare(sql, Placeholders.of("o_id", long.class)).get();

        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        ps2.close();
        ps4.close();

        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        link.next(ProtosForTest.ResultOnlyResponseChecker.builder().build());
        sqlClient.close();
        session.close();
        session.waitForCompletion();

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
        var wire = new WireImpl(link);
        var session = new SessionImpl();
        session.connect(wire);
        var sqlClient = SqlClient.attach(session);

        link.next(ProtosForTest.BeginResponseChecker.builder().build());
        link.setTimeoutOnEmpty(true);

        var transaction = sqlClient.createTransaction().get();
        var future = transaction.executeStatement("this is a sql for test");
        future.close();

        var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
        assertEquals(SERVICE_ID_ENDPOINT_BROKER, header.getServiceId());
        var request = EndpointRequest.Request.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforePayload()));
        assertTrue(request.hasCancel());
    }
}
