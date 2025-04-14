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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.sql.proto.SqlError;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.FileBlobInfo;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.SearchPath;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class SqlServiceStubTest {
    private static final String RS_RD = "relation"; //$NON-NLS-1$

    private final MockWire wire = new MockWire();

    private final Session session = new SessionImpl();

    public SqlServiceStubTest() {
        session.connect(wire);
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    private static RequestHandler accepts(SqlRequest.Request.RequestCase command, RequestHandler next) {
        return new RequestHandler() {
            @Override
            public Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException {
                SqlRequest.Request message = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertEquals(command, message.getRequestCase());
                return next.handle(serviceId, request);
            }
        };
    }

    private static SqlResponse.Success newVoid() {
        return SqlResponse.Success.newBuilder()
                .build();
    }

    private static SqlResponse.Error newEngineError() {
        return SqlResponse.Error.newBuilder()
                .setCode(SqlError.Code.SQL_SERVICE_EXCEPTION)
                .build();
    }

    private static SqlResponse.ResultSetMetadata toResultSetMetadata(SqlCommon.Column... columns) {
        return SqlResponse.ResultSetMetadata.newBuilder()
                .addAllColumns(Arrays.asList(columns))
                .build();
    }

//    private SchemaProtos.RecordMeta toResultSetMetadata(SqlCommon.Column... columns) {
//        var builder = SchemaProtos.RecordMeta.newBuilder();
//        for (var e: Arrays.asList(columns)) {
//            var column = SchemaProtos.RecordMeta.Column.newBuilder()
//            .setName(e.getName())
//            .setType(e.getAtomType());
//            builder.addColumns(column);
//
//        }
//        return builder.build();
//    }

    @Test
    void sendBeginSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BEGIN,
                RequestHandler.returns(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                            .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                .setHandle(100)))
                        .build())));

        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));

            // add handler for Rollback
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasRollback());
                var rollback = req.getRollback();
                assertEquals(100, rollback.getTransactionHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.ResultOnly.newBuilder()
                    .setSuccess(newVoid())
                    .build())));
            });

            // add handler for DisposeTransaction
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasDisposeTransaction());
                var disposeTransaction = req.getDisposeTransaction();
                assertEquals(100, disposeTransaction.getTransactionHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.DisposeTransaction.newBuilder()
                    .setSuccess(SqlResponse.Void.newBuilder().build())
                    .build())));
            });

        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendBeginSuccessAutoclose() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BEGIN,
                RequestHandler.returns(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                            .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                .setHandle(100)))
                        .build())));

        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var tx = future.await();
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));

            // don't close
            // tx.close();

            // add handler for Rollback
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasRollback());
                var rollback = req.getRollback();
                assertEquals(100, rollback.getTransactionHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.ResultOnly.newBuilder()
                    .setSuccess(newVoid())
                    .build())));
            });

            // add handler for DisposeTransaction
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasDisposeTransaction());
                var disposeTransaction = req.getDisposeTransaction();
                assertEquals(100, disposeTransaction.getTransactionHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.DisposeTransaction.newBuilder()
                    .setSuccess(SqlResponse.Void.newBuilder().build())
                    .build())));
            });
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendBeginCloseFutureResponse() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BEGIN,
                RequestHandler.returns(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                            .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                .setHandle(100)))
                        .build())));

        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            // add handler for Rollback
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasRollback());
                var rollback = req.getRollback();
                assertEquals(100, rollback.getTransactionHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.ResultOnly.newBuilder()
                    .setSuccess(newVoid())
                    .build())));
            });

            // add handler for DisposeTransaction
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasDisposeTransaction());
                var disposeTransaction = req.getDisposeTransaction();
                assertEquals(100, disposeTransaction.getTransactionHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.DisposeTransaction.newBuilder()
                    .setSuccess(SqlResponse.Void.newBuilder().build())
                    .build())));
            });

            // close FutureResponse without invoking future.get()
            future.close();

            // Delay the session close,
            // otherwise the disposer thread encounter the closed session
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendBeginEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BEGIN,
                RequestHandler.returns(SqlResponse.Begin.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

//    @Test  FIXME
    void sendBeginBroken() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BEGIN,
                RequestHandler.returns(SqlResponse.Begin.newBuilder()
                        .build())));

        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            assertThrows(BrokenResponseException.class, () -> future.await());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendCommitSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.COMMIT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        var message = SqlRequest.Commit.newBuilder()
                .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            assertDoesNotThrow(() -> future.get());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendCommitEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.COMMIT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.Commit.newBuilder()
                .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    static class TransactionImplTest extends TransactionImpl {
        private boolean notified = false;
        public TransactionImplTest(SqlService service) {
            super(null, service, null);
        }
        boolean isNotified() {
            return notified;
        }
        @Override
        void notifyCommitSuccess() {
            notified = true;
        }
        @Override
        public void close() {
        }
    }

    @Test
    void sendCommitSuccessAutoDispose() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.COMMIT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        var message = SqlRequest.Commit.newBuilder()
                .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImplTest(service);
            var future = service.send(message, transaction);
        ) {
            assertDoesNotThrow(() -> future.get());
            assertTrue(transaction.isNotified());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendCommitEngineErrorAutoDispose() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.COMMIT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.Commit.newBuilder()
                .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImplTest(service);
            var future = service.send(message, transaction);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
            assertFalse(transaction.isNotified());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendRollbackSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.ROLLBACK,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        var message = SqlRequest.Rollback.newBuilder()
                .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            assertDoesNotThrow(() -> future.get());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendRollbackEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.ROLLBACK,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.Rollback.newBuilder()
                .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendPrepareSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.PREPARE,
                RequestHandler.returns(SqlResponse.Prepare.newBuilder()
                        .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder()
                                .setHandle(100))
                        .build())));

        var message = SqlRequest.Prepare.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var stmt = future.await();
        ) {
            assertEquals(100, ((PreparedStatementImpl) stmt).getHandle().getHandle());

            // add handler for DisposePreparedStatement
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasDisposePreparedStatement());
                var dispose = req.getDisposePreparedStatement();
                assertEquals(100, dispose.getPreparedStatementHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.ResultOnly.newBuilder()
                    .setSuccess(newVoid())
                    .build())));
            });
        }
        assertFalse(wire.hasRemaining());
    }

    private static byte[] toDelimitedByteArray(SqlResponse.ResultOnly response) {
        var sqlResponse = SqlResponse.Response.newBuilder().setResultOnly(response).build();

        try (var buffer = new ByteArrayOutputStream()) {
            sqlResponse.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            System.err.println(e);
            e.printStackTrace();
        }
        return null;
    }

    private static byte[] toDelimitedByteArray(SqlResponse.DisposeTransaction response) {
        var sqlResponse = SqlResponse.Response.newBuilder().setDisposeTransaction(response).build();

        try (var buffer = new ByteArrayOutputStream()) {
            sqlResponse.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            System.err.println(e);
            e.printStackTrace();
        }
        return null;
    }

    @Test
    void sendPrepareSuccessAutoclose() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.PREPARE,
                RequestHandler.returns(SqlResponse.Prepare.newBuilder()
                        .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                        .build())));

        var message = SqlRequest.Prepare.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var stmt = future.await();
            assertEquals(100, ((PreparedStatementImpl) stmt).getHandle().getHandle());

            // don't close prepared statement
            // stmt.close();

            // add handler for DisposePreparedStatement
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasDisposePreparedStatement());
                var dispose = req.getDisposePreparedStatement();
                assertEquals(100, dispose.getPreparedStatementHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.ResultOnly.newBuilder()
                .setSuccess(newVoid())
                .build())));
            });
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendPrepareCloseFutureResponse() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.PREPARE,
        RequestHandler.returns(SqlResponse.Prepare.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                .build())));

        var message = SqlRequest.Prepare.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            // add handler for DisposePreparedStatement
            wire.next((id, request) -> {
                var req = SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertTrue(req.hasDisposePreparedStatement());
                var dispose = req.getDisposePreparedStatement();
                assertEquals(100, dispose.getPreparedStatementHandle().getHandle());
                return new SimpleResponse(ByteBuffer.wrap(toDelimitedByteArray(SqlResponse.ResultOnly.newBuilder()
                .setSuccess(newVoid())
                .build())));
            });

            // close FutureResponse without invoking future.get()
            future.close();

            // Delay the session close,
            // otherwise the disposer thread encounter the closed session
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendPrepareEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.PREPARE,
                RequestHandler.returns(SqlResponse.Prepare.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.Prepare.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDisposePreparedStatementSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DISPOSE_PREPARED_STATEMENT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        var message = SqlRequest.DisposePreparedStatement.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            assertDoesNotThrow(() -> future.get());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDisposePreparedStatementEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DISPOSE_PREPARED_STATEMENT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.DisposePreparedStatement.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExplainByTextSuccess() throws Exception {
        var columns = List.of(
                Types.column("a", Types.of(BigDecimal.class)),
                Types.column("b", Types.of(String.class)),
                Types.column("c", Types.of(double.class)));
        wire.next(accepts(SqlRequest.Request.RequestCase.EXPLAIN_BY_TEXT,
                RequestHandler.returns(SqlResponse.Explain.newBuilder()
                        .setSuccess(SqlResponse.Explain.Success.newBuilder()
                                .setFormatId("T")
                                .setFormatVersion(123)
                                .setContents("TESTING")
                                .addAllColumns(columns))
                        .build())));

        var message = SqlRequest.ExplainByText.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals("T", result.getFormatId());
            assertEquals(123, result.getFormatVersion());
            assertEquals("TESTING", result.getContents());
            assertEquals(columns, result.getColumns());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExplainByTextOutput() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXPLAIN_BY_TEXT,
                RequestHandler.returns(SqlResponse.Explain.newBuilder()
                    .setSuccess(SqlResponse.Explain.Success.newBuilder()
                        .setFormatId(SqlServiceStub.FORMAT_ID_LEGACY_EXPLAIN)
                        .setFormatVersion(SqlServiceStub.FORMAT_VERSION_LEGACY_EXPLAIN)
                        .setContents("TESTING"))
                    .build())));

        var message = SqlRequest.ExplainByText.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals(SqlServiceStub.FORMAT_ID_LEGACY_EXPLAIN, result.getFormatId());
            assertEquals(SqlServiceStub.FORMAT_VERSION_LEGACY_EXPLAIN, result.getFormatVersion());
            assertEquals("TESTING", result.getContents());
            assertEquals(List.of(), result.getColumns());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExplainByTextError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXPLAIN_BY_TEXT,
                RequestHandler.returns(SqlResponse.Explain.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.ExplainByText.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            assertThrows(SqlServiceException.class, () -> future.get());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExplainSuccess() throws Exception {
        var columns = List.of(
                Types.column("a", Types.of(BigDecimal.class)),
                Types.column("b", Types.of(String.class)),
                Types.column("c", Types.of(double.class)));
        wire.next(accepts(SqlRequest.Request.RequestCase.EXPLAIN,
                RequestHandler.returns(SqlResponse.Explain.newBuilder()
                        .setSuccess(SqlResponse.Explain.Success.newBuilder()
                                .setFormatId("T")
                                .setFormatVersion(123)
                                .setContents("TESTING")
                                .addAllColumns(columns))
                        .build())));

        var message = SqlRequest.Explain.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals("T", result.getFormatId());
            assertEquals(123, result.getFormatVersion());
            assertEquals("TESTING", result.getContents());
            assertEquals(columns, result.getColumns());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExplainOutput() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXPLAIN,
                RequestHandler.returns(SqlResponse.Explain.newBuilder()
                    .setSuccess(SqlResponse.Explain.Success.newBuilder()
                        .setFormatId(SqlServiceStub.FORMAT_ID_LEGACY_EXPLAIN)
                        .setFormatVersion(SqlServiceStub.FORMAT_VERSION_LEGACY_EXPLAIN)
                        .setContents("TESTING"))
                    .build())));

        var message = SqlRequest.Explain.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals(SqlServiceStub.FORMAT_ID_LEGACY_EXPLAIN, result.getFormatId());
            assertEquals(SqlServiceStub.FORMAT_VERSION_LEGACY_EXPLAIN, result.getFormatVersion());
            assertEquals("TESTING", result.getContents());
            assertEquals(List.of(), result.getColumns());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExplainError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXPLAIN,
                RequestHandler.returns(SqlResponse.Explain.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.Explain.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            assertThrows(SqlServiceException.class, () -> future.get());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDescribeTableSuccess() throws Exception {
        var columns = List.of(
                Types.column("a", Types.of(BigDecimal.class)),
                Types.column("b", Types.of(String.class)),
                Types.column("c", Types.of(double.class)));
        wire.next(accepts(SqlRequest.Request.RequestCase.DESCRIBE_TABLE,
                RequestHandler.returns(SqlResponse.DescribeTable.newBuilder()
                        .setSuccess(SqlResponse.DescribeTable.Success.newBuilder()
                                .setDatabaseName("A")
                                .setSchemaName("B")
                                .setTableName("C")
                                .addAllColumns(columns))
                        .build())));

        var message = SqlRequest.DescribeTable.newBuilder()
                .setName("T")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals(Optional.of("A"), result.getDatabaseName());
            assertEquals(Optional.of("B"), result.getSchemaName());
            assertEquals("C", result.getTableName());
            assertEquals(columns, result.getColumns());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDescribeTableEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DESCRIBE_TABLE,
                RequestHandler.returns(SqlResponse.DescribeTable.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.DescribeTable.newBuilder()
                .setName("T")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExecuteSuccess_deprecated() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_STATEMENT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        var message = SqlRequest.ExecuteStatement.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var executeResult = future.get();
            var counterTypes = executeResult.getCounterTypes();
            assertTrue(counterTypes.isEmpty());
            var counters = executeResult.getCounters();
            assertTrue(counters.isEmpty());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExecuteEngineError_deprecated() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_STATEMENT,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.ExecuteStatement.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExecuteSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_STATEMENT,
                RequestHandler.returns(SqlResponse.ExecuteResult.newBuilder()
                        .setSuccess(SqlResponse.ExecuteResult.Success.newBuilder()
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.INSERTED_ROWS).setValue(1))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.UPDATED_ROWS).setValue(2))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.MERGED_ROWS).setValue(3))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.DELETED_ROWS).setValue(4)))
                        .build())));

        var message = SqlRequest.ExecuteStatement.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var executeResult = future.get();
            var counterTypes = executeResult.getCounterTypes();
            assertTrue(counterTypes.containsAll(List.of(CounterType.INSERTED_ROWS, CounterType.UPDATED_ROWS, CounterType.MERGED_ROWS, CounterType.DELETED_ROWS)));
            var counters = executeResult.getCounters();
            assertEquals(counters.get(CounterType.INSERTED_ROWS), 1);
            assertEquals(counters.get(CounterType.UPDATED_ROWS), 2);
            assertEquals(counters.get(CounterType.MERGED_ROWS), 3);
            assertEquals(counters.get(CounterType.DELETED_ROWS), 4);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExecuteEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_STATEMENT,
                RequestHandler.returns(SqlResponse.ExecuteResult.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.ExecuteStatement.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExecutePreparedStatementWithLobSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_PREPARED_STATEMENT,
                RequestHandler.returns(SqlResponse.ExecuteResult.newBuilder()
                        .setSuccess(SqlResponse.ExecuteResult.Success.newBuilder()
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.INSERTED_ROWS).setValue(1))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.UPDATED_ROWS).setValue(2))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.MERGED_ROWS).setValue(3))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.DELETED_ROWS).setValue(4)))
                        .build())));

        var message = SqlRequest.ExecutePreparedStatement.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(12345).build())
                .build();
        var lobs = new LinkedList<FileBlobInfo>();
        lobs.add(new FileBlobInfo("blobChannel", Path.of("/somewhere/blobChannel.data")));
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message, lobs);
        ) {
            var executeResult = future.get();
            var counterTypes = executeResult.getCounterTypes();
            assertTrue(counterTypes.containsAll(List.of(CounterType.INSERTED_ROWS, CounterType.UPDATED_ROWS, CounterType.MERGED_ROWS, CounterType.DELETED_ROWS)));
            var counters = executeResult.getCounters();
            assertEquals(counters.get(CounterType.INSERTED_ROWS), 1);
            assertEquals(counters.get(CounterType.UPDATED_ROWS), 2);
            assertEquals(counters.get(CounterType.MERGED_ROWS), 3);
            assertEquals(counters.get(CounterType.DELETED_ROWS), 4);

            var lob = wire.blobs().get(0);
            assertEquals(lob.getChannelName(), "blobChannel");
            assertEquals(lob.getPath().get().toString(), "/somewhere/blobChannel.data");
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendBatchSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BATCH,
                RequestHandler.returns(SqlResponse.ExecuteResult.newBuilder()
                        .setSuccess(SqlResponse.ExecuteResult.Success.newBuilder()
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.INSERTED_ROWS).setValue(1))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.UPDATED_ROWS).setValue(2))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.MERGED_ROWS).setValue(3))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.DELETED_ROWS).setValue(4)))
                        .build())));

        var message = SqlRequest.Batch.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var executeResult = future.get();
            var counterTypes = executeResult.getCounterTypes();
            assertTrue(counterTypes.containsAll(List.of(CounterType.INSERTED_ROWS, CounterType.UPDATED_ROWS, CounterType.MERGED_ROWS, CounterType.DELETED_ROWS)));
            var counters = executeResult.getCounters();
            assertEquals(counters.get(CounterType.INSERTED_ROWS), 1);
            assertEquals(counters.get(CounterType.UPDATED_ROWS), 2);
            assertEquals(counters.get(CounterType.MERGED_ROWS), 3);
            assertEquals(counters.get(CounterType.DELETED_ROWS), 4);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendBatchEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BATCH,
                RequestHandler.returns(SqlResponse.ExecuteResult.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.Batch.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendQuerySuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_QUERY,
                RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build(),
                        toResultSetMetadata(
                            Types.column("a", Types.of(BigDecimal.class)),
                            Types.column("b", Types.of(String.class)),
                            Types.column("c", Types.of(double.class))).toByteArray(),
                        Relation.of(new Object[][] {
                            {
                                new BigDecimal("3.14"),
                                "Hello, world!",
                                1.25,
                            },
                        }))));

        var message = SqlRequest.ExecuteQuery.newBuilder()
                .setSql("SELECT 1")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var rs = future.await();
        ) {
            assertTrue(rs.nextRow());

            assertTrue(rs.nextColumn());
            assertEquals(new BigDecimal("3.14"), rs.fetchDecimalValue());

            assertTrue(rs.nextColumn());
            assertEquals("Hello, world!", rs.fetchCharacterValue());

            assertTrue(rs.nextColumn());
            assertEquals(1.25d, rs.fetchFloat8Value());

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRow());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void useResultSet_after_transaction_close() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.BEGIN,
                RequestHandler.returns(
                        SqlResponse.Begin.newBuilder()
                                .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                                        .setHandle(123))).build())));
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_QUERY,
                RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build(),
                        toResultSetMetadata(
                            Types.column("a", Types.of(BigDecimal.class)),
                            Types.column("b", Types.of(String.class)),
                            Types.column("c", Types.of(double.class))).toByteArray(),
                        Relation.of(new Object[][] {
                            {
                                new BigDecimal("3.14"),
                                "Hello, world!",
                                1.25,
                            },
                        }))));
        wire.next(accepts(SqlRequest.Request.RequestCase.COMMIT,
                RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build())));
        wire.next(accepts(SqlRequest.Request.RequestCase.DISPOSE_TRANSACTION,
                RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build())));

        try (
            var service = new SqlServiceStub(session);
            var transaction = service.send(SqlRequest.Begin.newBuilder().build()).await();
            var future = transaction.executeQuery("SELECT 1");
            var rs = future.await();
        ) {
            transaction.commit().await();

            // expects transaction already closed
            assertThrows(IOException.class, () -> rs.nextRow());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendPreparedQuerySuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_PREPARED_QUERY,
                RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build(),
                        toResultSetMetadata(
                            Types.column("a", Types.of(BigDecimal.class)),
                            Types.column("b", Types.of(String.class)),
                            Types.column("c", Types.of(double.class))).toByteArray(),
                        Relation.of(new Object[][] {
                            {
                                new BigDecimal("3.14"),
                                "Hello, world!",
                                1.25,
                            },
                        }))));

        var message = SqlRequest.ExecutePreparedQuery.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(12345).build())
                .build();
        var lobs = new LinkedList<FileBlobInfo>();
        lobs.add(new FileBlobInfo("blobChannel", Path.of("/somewhere/blobChannel.data")));
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message, lobs);
            var rs = future.await();
        ) {
            assertTrue(rs.nextRow());

            assertTrue(rs.nextColumn());
            assertEquals(new BigDecimal("3.14"), rs.fetchDecimalValue());

            assertTrue(rs.nextColumn());
            assertEquals("Hello, world!", rs.fetchCharacterValue());

            assertTrue(rs.nextColumn());
            assertEquals(1.25d, rs.fetchFloat8Value());

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRow());

            var lob = wire.blobs().get(0);
            assertEquals(lob.getChannelName(), "blobChannel");
            assertEquals(lob.getPath().get().toString(), "/somewhere/blobChannel.data");
        }
        assertFalse(wire.hasRemaining());
    }

//    @Test
//    void sendQuerySuccess_with_metadata_broken() throws Exception {
//        wire.next(accepts(SqlRequest.Request.RequestCase.QUERY,
//                RequestHandler.returns(
//                        SqlResponse.Query.newBuilder()
//                                .setSuccess(newVoid())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_MD, new byte[] { (byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe,}),
//                        Map.entry(RS_RD, Relation.of(new Object[][] {
//                            {
//                                new BigDecimal("3.14"),
//                                "Hello, world!",
//                                1.25,
//                            },
//                        }).getBytes()))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//        ) {
//            assertThrows(IOException.class, () -> future.await());
//        }
//        assertFalse(wire.hasRemaining());
//    }
//
//    @Test
//    void sendQuerySuccess_with_relation_broken() throws Exception {
//        wire.next(accepts(SqlRequest.Request.RequestCase.QUERY,
//                RequestHandler.returns(
//                        SqlResponse.Query.newBuilder()
//                                .setSuccess(newVoid())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_MD, toResultSetMetadataBytes(
//                                Types.column("a", Types.of(BigDecimal.class)),
//                                Types.column("b", Types.of(String.class)),
//                                Types.column("c", Types.of(double.class)))),
//                        Map.entry(RS_RD, new byte[] { (byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe,}))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//            var rs = future.await();
//        ) {
//            assertThrows(IOException.class, () -> rs.nextRow());
//        }
//        assertFalse(wire.hasRemaining());
//    }

    @Test
    void sendQuerySuccessWithoutMetadata() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_QUERY,
                RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder()
                            .setError(newEngineError())
                            .build(),
                        Relation.of(new Object[][] {
                            {
                                new BigDecimal("3.14"),
                                "Hello, world!",
                                1.25,
                            },
                        }))));

        var message = SqlRequest.ExecuteQuery.newBuilder()
            .setSql("SELECT 1")
            .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            assertThrows(SqlServiceException.class, () -> future.await().close());
        }
        assertFalse(wire.hasRemaining());
    }

//    @Test
//    void sendQuerySuccess_without_relation() throws Exception {
//        wire.next(accepts(
//                SqlRequest.Request.RequestCase.QUERY,
//                        RequestHandler.returns(SqlResponse.Query.newBuilder()
//                                .setSuccess(newVoid())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_MD, toResultSetMetadataBytes(
//                                Types.column("a", Types.of(BigDecimal.class)),
//                                Types.column("b", Types.of(String.class)),
//                                Types.column("c", Types.of(double.class)))))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//        ) {
//            assertThrows(IOException.class, () -> future.await());
//        }
//        assertFalse(wire.hasRemaining());
//    }
//
//    @Test
//    void sendQueryEngineError_with_correct_subs() throws Exception {
//        wire.next(accepts(SqlRequest.Request.RequestCase.QUERY,
//                RequestHandler.returns(
//                        SqlResponse.Query.newBuilder()
//                                .setEngineError(newEngineError())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_MD, toResultSetMetadataBytes(
//                                Types.column("a", Types.of(BigDecimal.class)),
//                                Types.column("b", Types.of(String.class)),
//                                Types.column("c", Types.of(double.class)))),
//                        Map.entry(RS_RD, Relation.of(new Object[][] {
//                            {
//                                new BigDecimal("3.14"),
//                                "Hello, world!",
//                                1.25,
//                            },
//                        }).getBytes()))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//            var rs = future.await();
//        ) {
//            var error = assertThrows(SqlServiceException.class, () -> {
//                assertTrue(rs.nextRow());
//
//                assertTrue(rs.nextColumn());
//                assertEquals(new BigDecimal("3.14"), rs.fetchDecimalValue());
//
//                assertTrue(rs.nextColumn());
//                assertEquals("Hello, world!", rs.fetchCharacterValue());
//
//                assertTrue(rs.nextColumn());
//                assertEquals(1.25d, rs.fetchFloat8Value());
//
//                assertFalse(rs.nextColumn());
//                assertFalse(rs.nextRow());
//
//                rs.close();
//            });
//            assertEquals(SqlServiceCode.UNKNOWN, error.getDiagnosticCode());
//        }
//        assertFalse(wire.hasRemaining());
//    }
//
//    @Test
//    void sendQueryEngineError_with_metadata_broken() throws Exception {
//        wire.next(accepts(SqlRequest.Request.RequestCase.QUERY,
//                RequestHandler.returns(
//                        SqlResponse.Query.newBuilder()
//                                .setEngineError(newEngineError())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_MD, new byte[] { (byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe,}),
//                        Map.entry(RS_RD, Relation.of(new Object[][] {
//                            {
//                                new BigDecimal("3.14"),
//                                "Hello, world!",
//                                1.25,
//                            },
//                        }).getBytes()))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//        ) {
//            var error = assertThrows(SqlServiceException.class, () -> future.await());
//            assertEquals(SqlServiceCode.UNKNOWN, error.getDiagnosticCode());
//        }
//        assertFalse(wire.hasRemaining());
//    }
//
//    @Test
//    void sendQueryEngineError_with_relation_broken() throws Exception {
//        wire.next(accepts(SqlRequest.Request.RequestCase.QUERY,
//                RequestHandler.returns(
//                        SqlResponse.Query.newBuilder()
//                                .setEngineError(newEngineError())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_MD, toResultSetMetadataBytes(
//                                Types.column("a", Types.of(BigDecimal.class)),
//                                Types.column("b", Types.of(String.class)),
//                                Types.column("c", Types.of(double.class)))),
//                        Map.entry(RS_RD, new byte[] { (byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe,}))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//            var rs = future.await();
//        ) {
//            var error = assertThrows(SqlServiceException.class, () -> rs.nextRow());
//            assertEquals(SqlServiceCode.UNKNOWN, error.getDiagnosticCode());
//        }
//        assertFalse(wire.hasRemaining());
//    }
//
//    @Test
//    void sendQueryEngineError_without_metadata() throws Exception {
//        wire.next(accepts(
//                SqlRequest.Request.RequestCase.QUERY,
//                        RequestHandler.returns(SqlResponse.Query.newBuilder()
//                                .setEngineError(newEngineError())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_RD, Relation.of(new Object[][] {
//                            {
//                                new BigDecimal("3.14"),
//                                "Hello, world!",
//                                1.25,
//                            },
//                        }).getBytes()))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//        ) {
//            var error = assertThrows(SqlServiceException.class, () -> future.await());
//            assertEquals(SqlServiceCode.UNKNOWN, error.getDiagnosticCode());
//        }
//        assertFalse(wire.hasRemaining());
//    }
//
//    @Test
//    void sendQueryEngineError_without_relation() throws Exception {
//        wire.next(accepts(
//                SqlRequest.Request.RequestCase.QUERY,
//                        RequestHandler.returns(SqlResponse.Query.newBuilder()
//                                .setEngineError(newEngineError())
//                                .build()
//                                .toByteArray(),
//                        Map.entry(RS_MD, toResultSetMetadataBytes(
//                                Types.column("a", Types.of(BigDecimal.class)),
//                                Types.column("b", Types.of(String.class)),
//                                Types.column("c", Types.of(double.class)))))));
//
//        var message = SqlRequest.Query.newBuilder()
//                .setSql("SELECT 1")
//                .setResultSet(RS_REQ)
//                .build();
//        try (
//            var service = new SqlServiceStub(session);
//            var future = service.send(message);
//        ) {
//            var error = assertThrows(SqlServiceException.class, () -> future.await());
//            assertEquals(SqlServiceCode.UNKNOWN, error.getDiagnosticCode());
//        }
//        assertFalse(wire.hasRemaining());
//    }

    @Test
    void sendDumpSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_DUMP,
                RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build(),
                        toResultSetMetadata(
                            Types.column("path", Types.of(String.class))).toByteArray(),
                        Relation.of(new Object[][] {
                            { "a" },
                            { "b" },
                            { "c" },
                        }))));

        var message = SqlRequest.ExecuteDump.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder())
                .setDirectory("/path/to/dump")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var rs = future.await();
        ) {
            assertTrue(rs.nextRow());
            assertTrue(rs.nextColumn());
            assertEquals("a", rs.fetchCharacterValue());
            assertFalse(rs.nextColumn());

            assertTrue(rs.nextRow());
            assertTrue(rs.nextColumn());
            assertEquals("b", rs.fetchCharacterValue());
            assertFalse(rs.nextColumn());

            assertTrue(rs.nextRow());
            assertTrue(rs.nextColumn());
            assertEquals("c", rs.fetchCharacterValue());
            assertFalse(rs.nextColumn());

            assertFalse(rs.nextRow());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDumpEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_DUMP,
                    RequestHandler.returns(
                        SqlResponse.ResultOnly.newBuilder()
                            .setError(newEngineError())
                        .build(),
                    Relation.of(new Object[][] {
                        { "a" },
                        { "b" },
                        { "c" },
                    }))));

        var message = SqlRequest.ExecuteDump.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder())
                .setDirectory("/path/to/dump")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await().close());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendLoadSuccess_deprecated() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_LOAD,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        var message = SqlRequest.ExecuteLoad.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                .addFile("a")
                .addFile("b")
                .addFile("c")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var executeResult = future.get();
            var counterTypes = executeResult.getCounterTypes();
            assertTrue(counterTypes.isEmpty());
            var counters = executeResult.getCounters();
            assertTrue(counters.isEmpty());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendLoadEngineError_deprecated() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_LOAD,
                RequestHandler.returns(SqlResponse.ResultOnly.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.ExecuteLoad.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                .addFile("a")
                .addFile("b")
                .addFile("c")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendLoadSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_LOAD,
                RequestHandler.returns(SqlResponse.ExecuteResult.newBuilder()
                        .setSuccess(SqlResponse.ExecuteResult.Success.newBuilder()
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.INSERTED_ROWS).setValue(1))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.UPDATED_ROWS).setValue(2))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.MERGED_ROWS).setValue(3))
                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.DELETED_ROWS).setValue(4)))
                        .build())));

        var message = SqlRequest.ExecuteLoad.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                .addFile("a")
                .addFile("b")
                .addFile("c")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var executeResult = future.get();
            var counterTypes = executeResult.getCounterTypes();
            assertTrue(counterTypes.containsAll(List.of(CounterType.INSERTED_ROWS, CounterType.UPDATED_ROWS, CounterType.MERGED_ROWS, CounterType.DELETED_ROWS)));
            var counters = executeResult.getCounters();
            assertEquals(counters.get(CounterType.INSERTED_ROWS), 1);
            assertEquals(counters.get(CounterType.UPDATED_ROWS), 2);
            assertEquals(counters.get(CounterType.MERGED_ROWS), 3);
            assertEquals(counters.get(CounterType.DELETED_ROWS), 4);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendLoadEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.EXECUTE_LOAD,
                RequestHandler.returns(SqlResponse.ExecuteResult.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.ExecuteLoad.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(100))
                .addFile("a")
                .addFile("b")
                .addFile("c")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    static class SearchPathAdapterForTest implements SearchPath {
        private final List<String> schemaNames;

        private SearchPathAdapterForTest(List<String> schemaNames) {
            this.schemaNames = schemaNames;
        }

        static SearchPathAdapterForTest of(List<String> names) {
            return new SearchPathAdapterForTest(names);
        }

        static SearchPathAdapterForTest of(String... names) {
            return new SearchPathAdapterForTest(Arrays.asList(names));
        }

        @Override
        public List<String> getSchemaNames() {
            return schemaNames;
        }
    }

    @Test
    void sendListTablesSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.LISTTABLES,
                RequestHandler.returns(SqlResponse.ListTables.newBuilder()
                        .setSuccess(SqlResponse.ListTables.Success.newBuilder()
                                .addTablePathNames(SqlResponse.Name.newBuilder()
                                        .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("database1"))
                                        .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema1"))
                                        .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("table1")))
                                .addTablePathNames(SqlResponse.Name.newBuilder()
                                        .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("database2"))
                                        .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema2"))
                                        .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("table2"))))
                        .build())));

        var message = SqlRequest.ListTables.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            var tableNames = result.getTableNames();
            assertEquals(2, tableNames.size());
            assertEquals("database1.schema1.table1", tableNames.get(0));
            assertEquals("database2.schema2.table2", tableNames.get(1));
            var simepleNames = result.getSimpleNames(SearchPathAdapterForTest.of("database1", "schema1"));
            assertEquals(1, simepleNames.size());
            assertEquals("database1.schema1.table1", simepleNames.get(0));
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendListTablesEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.LISTTABLES,
                RequestHandler.returns(SqlResponse.ListTables.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.ListTables.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendSearchPathSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.GETSEARCHPATH,
                RequestHandler.returns(SqlResponse.GetSearchPath.newBuilder()
                        .setSuccess(SqlResponse.GetSearchPath.Success.newBuilder()
                                .addSearchPaths(SqlResponse.Name.newBuilder()
                                    .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema1")))
                                .addSearchPaths(SqlResponse.Name.newBuilder()
                                    .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema2"))))
                        .build())));

        var message = SqlRequest.GetSearchPath.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            var schemaNames = result.getSchemaNames();
            assertEquals(2, schemaNames.size());
            assertEquals("schema1", schemaNames.get(0));
            assertEquals("schema2", schemaNames.get(1));
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendSearchPathEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.GETSEARCHPATH,
                RequestHandler.returns(SqlResponse.GetSearchPath.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.GetSearchPath.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendGetErrorInfoSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.GET_ERROR_INFO,
                RequestHandler.returns(SqlResponse.GetErrorInfo.newBuilder()
                            .setSuccess(newEngineError())
                        .build())));

        var message = SqlRequest.GetErrorInfo.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals(result.getDiagnosticCode().getCodeNumber(), SqlServiceCode.SQL_SERVICE_EXCEPTION.getCodeNumber());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendGetErrorInfoErrorNotFound() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.GET_ERROR_INFO,
                RequestHandler.returns(SqlResponse.GetErrorInfo.newBuilder()
                        .setErrorNotFound(SqlResponse.Void.newBuilder())
                        .build())));

        var message = SqlRequest.GetErrorInfo.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals(result, null);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendGetErrorInfoEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.GET_ERROR_INFO,
                RequestHandler.returns(SqlResponse.GetErrorInfo.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.GetErrorInfo.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDisposeTransactionSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DISPOSE_TRANSACTION,
                RequestHandler.returns(SqlResponse.DisposeTransaction.newBuilder()
                        .setSuccess(SqlResponse.Void.newBuilder())
                        .build())));

        var message = SqlRequest.DisposeTransaction.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDisposeTransactionEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DISPOSE_TRANSACTION,
                RequestHandler.returns(SqlResponse.DisposeTransaction.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.DisposeTransaction.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendGetTableMetadataSuccess() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DESCRIBE_TABLE,
                RequestHandler.returns(SqlResponse.DescribeTable.newBuilder()
                        .setSuccess(SqlResponse.DescribeTable.Success.newBuilder()
                                    .setDatabaseName("D")
                                    .setSchemaName("S")
                                    .setTableName("TBL")
                                    .addColumns(Types.column("a", Types.of(int.class))))
                        .build())));

        var message = SqlRequest.DescribeTable.newBuilder()
                .setName("TBL")
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {

            var result = future.get();
            assertEquals(Optional.of("D"), result.getDatabaseName());
            assertEquals(Optional.of("S"), result.getSchemaName());
            assertEquals("TBL", result.getTableName());
            var columns = result.getColumns();
            assertEquals(1, columns.size());
            var column = columns.get(0);
            assertEquals("a", column.getName());
            assertEquals(SqlCommon.Column.TypeInfoCase.ATOM_TYPE, column.getTypeInfoCase());
            assertEquals(SqlCommon.AtomType.INT4, column.getAtomType());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendGetTableMetadataEngineError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DESCRIBE_TABLE,
                RequestHandler.returns(SqlResponse.DescribeTable.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.DescribeTable.newBuilder()
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var error = assertThrows(SqlServiceException.class, () -> future.await());
            assertEquals(SqlServiceCode.SQL_SERVICE_EXCEPTION, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }
}
