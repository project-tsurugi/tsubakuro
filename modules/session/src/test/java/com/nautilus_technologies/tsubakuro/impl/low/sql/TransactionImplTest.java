package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.tsurugidb.jogasaki.proto.SchemaProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SqlService;
import com.nautilus_technologies.tsubakuro.low.sql.Types;
import com.nautilus_technologies.tsubakuro.impl.low.sql.testing.Relation;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

class TransactionImplTest {

    @Test
    void commit() throws Exception {
        var closeCount = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Commit request) throws IOException {
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(null);
                }
            }, resource -> {
                closeCount.incrementAndGet();
            })
        ) {
            client.commit().await();
        }
        assertEquals(1, closeCount.get());
    }

    @Test
    void rollback() throws Exception {
        var rollbackCount = new AtomicInteger();
        var closeCount = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    rollbackCount.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(null);
                }
            }, resource -> {
                closeCount.incrementAndGet();
            })
        ) {
            client.rollback().await();
        }
        assertEquals(1, rollbackCount.get());
        assertEquals(1, closeCount.get());
    }

    @Test
    void executeText() throws Exception {
        var count = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.ExecuteStatement request) throws IOException {
                    count.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    assertEquals("SELECT 100", request.getSql());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, null);
        ) {
            client.executeStatement("SELECT 100").await();
        }
        assertEquals(1, count.get());
    }

    @Test
    void executeStatement() throws Exception {
        var count = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.ExecutePreparedStatement request) throws IOException {
                    count.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    assertEquals(100, request.getPreparedStatementHandle().getHandle());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, null);
        ) {
            client.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(100).build(), null),
            List.of()).await();
        }
        assertEquals(1, count.get());
    }

    @Test
    void queryText() throws Exception {
        var count = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<ResultSet> send(SqlRequest.ExecuteQuery request) throws IOException {
                    count.incrementAndGet();
                    return FutureResponse.returns(Relation.of(new Object[][] {
                        {
                            request.getTransactionHandle().getHandle(),
                            request.getSql(),
                        }
                    }).getResultSet(SchemaProtos.RecordMeta.newBuilder()
                            .addColumns(SchemaProtos.RecordMeta.Column.newBuilder()
                                .setName("transaction")
                                .setType(Types.of(long.class).getAtomType()))
                            .addColumns(SchemaProtos.RecordMeta.Column.newBuilder()
                                .setName("text")
                                .setType(Types.of(String.class).getAtomType())).build(),
                            new FutureResultOnly()));
                }
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, null);
            var rs = client.executeQuery("SELECT 1").await();
        ) {
            assertTrue(rs.nextRecord());
            assertTrue(rs.nextColumn());
            assertEquals(rs.getInt8(), 100);
            assertTrue(rs.nextColumn());
            assertEquals(rs.getCharacter(), "SELECT 1");

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRecord());
        }
        assertEquals(1, count.get());
    }

    @Test
    void queryStatement() throws Exception {
        var count = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<ResultSet> send(SqlRequest.ExecutePreparedQuery request) throws IOException {
                    count.incrementAndGet();
                    return FutureResponse.returns(Relation.of(new Object[][] {
                        {
                            request.getTransactionHandle().getHandle(),
                            request.getPreparedStatementHandle().getHandle(),
                        }
                    }).getResultSet(SchemaProtos.RecordMeta.newBuilder()
                            .addColumns(SchemaProtos.RecordMeta.Column.newBuilder()
                                .setName("transaction")
                                .setType(Types.of(long.class).getAtomType()))
                            .addColumns(SchemaProtos.RecordMeta.Column.newBuilder()
                                .setName("statement")
                                .setType(Types.of(long.class).getAtomType())).build(),
                            new FutureResultOnly()));
                }
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, null);
            var rs = client.executeQuery(prepared(200)).await();
        ) {
            assertTrue(rs.nextRecord());
            assertTrue(rs.nextColumn());
            assertEquals(rs.getInt8(), 100);
            assertTrue(rs.nextColumn());
            assertEquals(rs.getInt8(), 200);

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRecord());
        }
        assertEquals(1, count.get());
    }

//    @Test
//    void dump_text() throws Exception {
//        var count = new AtomicInteger();
//        try (
//            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
//                @Override
//                public FutureResponse<ResultSet> send(Dump request) throws IOException {
//                    count.incrementAndGet();
//                    return FutureResponse.returns(Relation.of(new Object[][] {
//                        {
//                            request.getTransactionId(),
//                            request.getSourceText(),
//                            request.getDirectories(0),
//                        }
//                    }).getResultSet(new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.newBuilder()
//                            .addColumns(Types.column("transaction", Types.of(long.class)))
//                            .addColumns(Types.column("text", Types.of(String.class)))
//                            .addColumns(Types.column("path", Types.of(String.class)))
//                            .build())));
//                }
//                @Override
//                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
//                    return FutureResponse.returns(null);
//                }
//            }, null);
//            var rs = client.dump("SELECT 1", Path.of("/path/to/dump")).await();
//        ) {
//            assertTrue(rs.nextRow());
//            assertTrue(rs.nextColumn());
//            assertEquals(rs.fetchInt8Value(), 100);
//            assertTrue(rs.nextColumn());
//            assertEquals(rs.fetchCharacterValue(), "SELECT 1");
//            assertTrue(rs.nextColumn());
//            assertEquals(path(rs.fetchCharacterValue()), path("/path/to/dump"));
//
//            assertFalse(rs.nextColumn());
//            assertFalse(rs.nextRow());
//        }
//        assertEquals(1, count.get());
//    }

    @Test
    void dumpStatement() throws Exception {
        var count = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<ResultSet> send(SqlRequest.ExecuteDump request) throws IOException {
                    count.incrementAndGet();
                    return FutureResponse.returns(Relation.of(new Object[][] {
                        {
                            request.getTransactionHandle().getHandle(),
                            request.getPreparedStatementHandle().getHandle(),
                            request.getDirectory(),
                        }
                    }).getResultSet(SchemaProtos.RecordMeta.newBuilder()
                            .addColumns(SchemaProtos.RecordMeta.Column.newBuilder()
                                .setName("transaction")
                                .setType(Types.of(long.class).getAtomType()))
                            .addColumns(SchemaProtos.RecordMeta.Column.newBuilder()
                                .setName("statement")
                                .setType(Types.of(long.class).getAtomType()))
                            .addColumns(SchemaProtos.RecordMeta.Column.newBuilder()
                                .setName("path")
                                .setType(Types.of(String.class).getAtomType())).build(),
                            new FutureResultOnly()));
                }
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, null);
            var rs = client.executeDump(prepared(200), List.of(), Path.of("/path/to/dump")).await();
        ) {
            assertTrue(rs.nextRecord());
            assertTrue(rs.nextColumn());
            assertEquals(rs.getInt8(), 100);
            assertTrue(rs.nextColumn());
            assertEquals(rs.getInt8(), 200);
            assertTrue(rs.nextColumn());
            assertEquals(path(rs.getCharacter()), path("/path/to/dump"));

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRecord());
        }
        assertEquals(1, count.get());
    }

    @Test
    void load() throws Exception {
        var count = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.ExecuteLoad request) throws IOException {
                    count.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    assertEquals(200, request.getPreparedStatementHandle().getHandle());
                    assertEquals(path("/path/to/load"), request.getFile(0));
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, null);
        ) {
            client.executeLoad(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(200).build(), null),
                                                            List.of(), Path.of("/path/to/load")).await();
        }
        assertEquals(1, count.get());
    }


    @Test
    void closeAuto() throws Exception {
        var rollbackCount = new AtomicInteger();
        var closeCount = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    rollbackCount.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, resource -> {
                closeCount.incrementAndGet();
            })
        ) {
            assertEquals(0, closeCount.get());  // to eliminate checkstyle warning
        }
        assertEquals(1, rollbackCount.get());
        assertEquals(1, closeCount.get());
    }

    @Test
    void closeMore() throws Exception {
        var rollbackCount = new AtomicInteger();
        var closeCount = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlCommon.Transaction.newBuilder().setHandle(100).build(), new SqlService() {
                @Override
                public FutureResponse<SqlResponse.ResultOnly> send(SqlRequest.Rollback request) throws IOException {
                    rollbackCount.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(SqlResponse.ResultOnly.newBuilder().setSuccess(newVoid()).build());
                }
            }, resource -> {
                closeCount.incrementAndGet();
            })
        ) {
            client.close();
            client.close();
        }
        assertEquals(1, rollbackCount.get());
        assertEquals(3, closeCount.get());  // FIXME confirm 1 or 3
    }

    private static PreparedStatementImpl prepared(long id) {
        return new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(id).build(), null);
    }

    private static String path(String string) {
        return Path.of(string).toAbsolutePath().toString();
    }

    private static SqlResponse.Success newVoid() {
        return SqlResponse.Success.newBuilder()
                .build();
    }

    private static byte[] toDelimitedByteArray(SqlResponse.ResultOnly response) {
        var sqlResponse = SqlResponse.Response.newBuilder().setResultOnly(response).build();

        try (var buffer = new ByteArrayOutputStream()) {
            sqlResponse.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
