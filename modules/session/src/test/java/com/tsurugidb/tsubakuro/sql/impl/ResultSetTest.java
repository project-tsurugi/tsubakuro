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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.framework.proto.FrameworkCommon;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlError;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.diagnostics.proto.Diagnostics;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.FileBlobInfo;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.util.FutureResponse;


class ResultSetTest {

    private final MockLink link = new MockLink();

    private WireImpl wire = null;

    private Session session = null;

    private Disposer disposer = null;

    ResultSetTest() {
        try {
            wire = new WireImpl(link);
            session = new SessionImpl();
            disposer = ((SessionImpl) session).disposer();
            session.connect(wire);
        } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        if (session != null) {
            session.close();
        }
    }

    @Test
    void query_Success() throws Exception {
        // for executeQuery
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build(),
                  "testResultSet",
                  SqlResponse.ResultSetMetadata.newBuilder()
                    .addColumns(SqlCommon.Column.newBuilder()
                                .setName("column1")
                                .setAtomType(SqlCommon.AtomType.INT8)
                                .build())
                    .build());
        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            var resultSet = transaction.executeQuery("select 1").get();
            assertFalse(resultSet.nextRow());  // also check no throw
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void query_applicationError_withResutSet() throws Exception {
        // for executeQuery
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setError(SqlResponse.Error.newBuilder()
                                    .setCode(SqlError.Code.CODE_UNSPECIFIED)))
                    .build(),
                  "testResultSet",
                  SqlResponse.ResultSetMetadata.newBuilder()
                    .addColumns(SqlCommon.Column.newBuilder()
                                .setName("column1")
                                .setAtomType(SqlCommon.AtomType.INT8)
                                .build())
                    .build());
        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            var resultSet = transaction.executeQuery("select 1").get();
            var e = assertThrows(SqlServiceException.class, () -> resultSet.nextRow());
        }
        disposer.waitForEmpty();
        assertTrue(link.getResultSetWire().isClosed());
        assertFalse(link.hasRemaining());
    }

    @Test
    void query_applicationError_withoutResultSet() throws Exception {
        // for executeQuery
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setError(SqlResponse.Error.newBuilder()
                                                .setCode(SqlError.Code.CODE_UNSPECIFIED)))
                    .build());
        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            var resultSet = transaction.executeQuery("select 1").get();
            var e = assertThrows(SqlServiceException.class, () -> resultSet.nextRow());
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void query_coreError_withResutSet() throws Exception {
        // for executeQuery
        link.next(FrameworkResponse.Header.newBuilder().setPayloadType(FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS).build(),
                    Diagnostics.Record.newBuilder().setCode(Diagnostics.Code.PERMISSION_ERROR).build(),
                  "testResultSet",
                  SqlResponse.ResultSetMetadata.newBuilder()
                    .addColumns(SqlCommon.Column.newBuilder()
                                .setName("column1")
                                .setAtomType(SqlCommon.AtomType.INT8)
                                .build())
                    .build());

        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            var resultSet = transaction.executeQuery("select 1").get();
            var e = assertThrows(CoreServiceException.class, () -> resultSet.nextRow());
            assertEquals(e.getDiagnosticCode(),  CoreServiceCode.valueOf(Diagnostics.Code.PERMISSION_ERROR));
        }
        disposer.waitForEmpty();
        assertTrue(link.getResultSetWire().isClosed());
        assertFalse(link.hasRemaining());
    }

    @Test
    void query_coreError_withoutResutSet() throws Exception {
        // for executeQuery
        link.next(FrameworkResponse.Header.newBuilder().setPayloadType(FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS).build(),
                    Diagnostics.Record.newBuilder().setCode(Diagnostics.Code.PERMISSION_ERROR).build());

        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            var resultSet = transaction.executeQuery("select 1").get();
            var e = assertThrows(CoreServiceException.class, () -> resultSet.nextRow());
            assertEquals(e.getDiagnosticCode(),  CoreServiceCode.valueOf(Diagnostics.Code.PERMISSION_ERROR));
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    static class Timeout extends Thread {
        private final FutureResponse<ResultSet> futureResultSet;
        private final int timeout;
        private Error error = null;
        private Exception exception = null;

        Timeout(FutureResponse<ResultSet> futureResultSet, int timeout) {
            this.futureResultSet = futureResultSet;
            this.timeout = timeout;
        }

        public void run() {
            try {
                assertTimeoutPreemptively(
                    Duration.ofSeconds(1), () -> { futureResultSet.get(timeout, TimeUnit.MILLISECONDS); }
                );
            } catch (Error e) {
                error = e;
            } catch (Exception e) {
                exception = e;
            }
        };

        Error getError() {
            return error;
        }
        Exception getException() {
            return exception;
        }
    }

    @Test
    void query_ZeroTimeout() throws Exception {
        link.setTimeoutOnEmpty(true);

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
            var futureResultSet = transaction.executeQuery("select 1");
        ) {
            var timeout = new Timeout(futureResultSet, 0);
            timeout.start();
            Thread.sleep(2000);
            timeout.join();

            assertTrue(timeout.getError() instanceof AssertionFailedError);
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void query_timeout() throws Exception {
        link.setTimeoutOnEmpty(true);

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
            var futureResultSet = transaction.executeQuery("select 1");
        ) {
            var timeout = new Timeout(futureResultSet, 500);
            timeout.start();
            Thread.sleep(2000);
            timeout.join();

            assertTrue(timeout.getException() instanceof ResponseTimeoutException);
        }
        assertFalse(link.hasRemaining());
    }
}
