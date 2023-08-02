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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.sql.proto.SqlStatus;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SearchPath;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class SqlServiceStubTest {
    private static final String RS_RD = "relation"; //$NON-NLS-1$

    private final MockWire wire = new MockWire();

    private final Session session = new SessionImpl(wire);

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
                .setStatus(SqlStatus.Status.ERR_UNKNOWN)
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
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

            // add handler for Rollback
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

            // add handler for Rollback
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDescribeStatementSuccess() throws Exception {
        var columns = List.of(
                Types.column("a", Types.of(BigDecimal.class)),
                Types.column("b", Types.of(String.class)),
                Types.column("c", Types.of(double.class)));
        wire.next(accepts(SqlRequest.Request.RequestCase.DESCRIBE_STATEMENT,
                RequestHandler.returns(SqlResponse.DescribeStatement.newBuilder()
                        .setSuccess(SqlResponse.DescribeStatement.Success.newBuilder()
                                .addAllColumns(columns)
                                .setPlan("TESTING"))
                        .build())));

        var message = SqlRequest.DescribeStatement.newBuilder()
                .setExecutableStatement(
                    SqlRequest.ExecutableStatement.newBuilder()
                        .setId(SqlCommon.PreparedStatement.getDefaultInstance().getHandle())
                    .build())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
        ) {
            var result = future.get();
            assertEquals(columns, result.getColumns());
            assertEquals("TESTING", result.getPlanText());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendDescribeStatementError() throws Exception {
        wire.next(accepts(SqlRequest.Request.RequestCase.DESCRIBE_STATEMENT,
                RequestHandler.returns(SqlResponse.DescribeStatement.newBuilder()
                        .setError(newEngineError())
                        .build())));

        var message = SqlRequest.DescribeStatement.newBuilder()
                .setExecutableStatement(
                        SqlRequest.ExecutableStatement.newBuilder()
                                .setId(SqlCommon.PreparedStatement.getDefaultInstance().getHandle())
                                .build())
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExecuteSuccess() throws Exception {
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
            assertDoesNotThrow(() -> future.get());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendExecuteEngineError() throws Exception {
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendLoadSuccess() throws Exception {
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
            assertDoesNotThrow(() -> future.get());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendLoadEngineError() throws Exception {
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void sendSearchPathuccess() throws Exception {
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
            assertEquals(SqlServiceCode.ERR_UNKNOWN, error.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }
}
