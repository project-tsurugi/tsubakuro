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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class TransactionImplTest {
    Disposer disposer = new Disposer();

    @Test
    void commit() throws Exception {
        var closeCount = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<Void> send(SqlRequest.Commit request) throws IOException {
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, resource -> {
                closeCount.incrementAndGet();
            }, disposer)
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
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    rollbackCount.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, resource -> {
                closeCount.incrementAndGet();
            }, disposer)
        ) {
            client.rollback().await();
        }
        disposer.waitForEmpty();
        assertEquals(1, rollbackCount.get());
        assertEquals(1, closeCount.get());
    }

    @Test
    void executeText() throws Exception {
        var count = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<ExecuteResult> send(SqlRequest.ExecuteStatement request) throws IOException {
                    count.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    assertEquals("SELECT 100", request.getSql());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, null, disposer);
        ) {
            client.executeStatement("SELECT 100").await();
        }
        assertEquals(1, count.get());
    }

    @Test
    void executeStatement() throws Exception {
        var count = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<ExecuteResult> send(SqlRequest.ExecutePreparedStatement request) throws IOException {
                    count.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    assertEquals(100, request.getPreparedStatementHandle().getHandle());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, null, disposer);
        ) {
            client.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(100).build(), null, null, null, disposer),
            List.of()).await();
        }
        assertEquals(1, count.get());
    }

    @Test
    void queryText() throws Exception {
        var count = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<ResultSet> send(SqlRequest.ExecuteQuery request) throws IOException {
                    count.incrementAndGet();
                    return FutureResponse.returns(Relation.of(new Object[][] {
                        {
                            request.getTransactionHandle().getHandle(),
                            request.getSql(),
                        }
                    }).getResultSet(new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.newBuilder()
                                        .addColumns(Types.column("transaction", Types.of(long.class)))
                                        .addColumns(Types.column("text", Types.of(String.class)))
                                        .build())));
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, null, disposer);
            var rs = client.executeQuery("SELECT 1").await();
        ) {
            assertTrue(rs.nextRow());
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchInt8Value(), 100);
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchCharacterValue(), "SELECT 1");

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRow());
        }
        assertEquals(1, count.get());
    }

    @Test
    void queryStatement() throws Exception {
        var count = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<ResultSet> send(SqlRequest.ExecutePreparedQuery request) throws IOException {
                    count.incrementAndGet();
                    return FutureResponse.returns(Relation.of(new Object[][] {
                        {
                            request.getTransactionHandle().getHandle(),
                            request.getPreparedStatementHandle().getHandle(),
                        }
                    }).getResultSet(new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.newBuilder()
                        .addColumns(Types.column("transaction", Types.of(long.class)))
                        .addColumns(Types.column("text", Types.of(String.class)))
                        .build())));
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, null, disposer);
            var rs = client.executeQuery(prepared(200, disposer)).await();
        ) {
            assertTrue(rs.nextRow());
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchInt8Value(), 100);
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchInt8Value(), 200);

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRow());
        }
        assertEquals(1, count.get());
    }

    @Test
    void dumpText() throws Exception {
        var count = new AtomicInteger();
        try (
            var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                            .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                            .build(),
                                            new SqlService() {
                @Override
                public FutureResponse<ResultSet> send(SqlRequest.ExecuteDumpByText request) throws IOException {
                    count.incrementAndGet();
                    return FutureResponse.returns(Relation.of(new Object[][] {
                        {
                            request.getTransactionHandle().getHandle(),
                            request.getSql(),
                            request.getDirectory(),
                        }
                    }).getResultSet(new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.newBuilder()
                            .addColumns(Types.column("transaction", Types.of(long.class)))
                            .addColumns(Types.column("text", Types.of(String.class)))
                            .addColumns(Types.column("path", Types.of(String.class)))
                            .build())));
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, null, disposer);
            var rs = client.executeDump("SELECT 1", Path.of("/path/to/dump")).await();
        ) {
            assertTrue(rs.nextRow());
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchInt8Value(), 100);
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchCharacterValue(), "SELECT 1");
            assertTrue(rs.nextColumn());
            assertEquals(path(rs.fetchCharacterValue()), path("/path/to/dump"));

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRow());
        }
        assertEquals(1, count.get());
    }

    @Test
    void dumpStatement() throws Exception {
        var count = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<ResultSet> send(SqlRequest.ExecuteDump request) throws IOException {
                    count.incrementAndGet();
                    return FutureResponse.returns(Relation.of(new Object[][] {
                        {
                            request.getTransactionHandle().getHandle(),
                            request.getPreparedStatementHandle().getHandle(),
                            request.getDirectory(),
                        }
                    }).getResultSet(new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.newBuilder()
                        .addColumns(Types.column("transaction", Types.of(long.class)))
                        .addColumns(Types.column("text", Types.of(String.class)))
                        .addColumns(Types.column("path", Types.of(String.class)))
                        .build())));
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, null, disposer);
            var rs = client.executeDump(prepared(200, disposer), List.of(), Path.of("/path/to/dump")).await();
        ) {
            assertTrue(rs.nextRow());
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchInt8Value(), 100);
            assertTrue(rs.nextColumn());
            assertEquals(rs.fetchInt8Value(), 200);
            assertTrue(rs.nextColumn());
            assertEquals(path(rs.fetchCharacterValue()), path("/path/to/dump"));

            assertFalse(rs.nextColumn());
            assertFalse(rs.nextRow());
        }
        assertEquals(1, count.get());
    }

    @Test
    void load() throws Exception {
        var count = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<ExecuteResult> send(SqlRequest.ExecuteLoad request) throws IOException {
                    count.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    assertEquals(200, request.getPreparedStatementHandle().getHandle());
                    assertEquals(path("/path/to/load"), request.getFile(0));
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, null, disposer);
        ) {
            client.executeLoad(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(200).build(), null, null, null, disposer),
                                                            List.of(), Path.of("/path/to/load")).await();
        }
        assertEquals(1, count.get());
    }


    @Test
    void closeAuto() throws Exception {
        var rollbackCount = new AtomicInteger();
        var closeCount = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    rollbackCount.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, resource -> {
                closeCount.incrementAndGet();
            }, disposer)
        ) {
            assertEquals(0, closeCount.get());  // to eliminate checkstyle warning
        }
        disposer.waitForEmpty();
        assertEquals(1, rollbackCount.get());
        assertEquals(1, closeCount.get());
    }

    @Test
    void closeMore() throws Exception {
        var rollbackCount = new AtomicInteger();
        var closeCount = new AtomicInteger();
        try (
             var client = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build())
                                              .build(),
                                              new SqlService() {
                @Override
                public FutureResponse<Void> send(SqlRequest.Rollback request) throws IOException {
                    rollbackCount.incrementAndGet();
                    assertEquals(100, request.getTransactionHandle().getHandle());
                    return FutureResponse.returns(null);
                }
                @Override
                public FutureResponse<Void> send(SqlRequest.DisposeTransaction request) throws IOException {
                    return FutureResponse.returns(null);
                }
            }, resource -> {
                closeCount.incrementAndGet();
            }, disposer)
        ) {
            client.close();
            client.close();
        }
        disposer.waitForEmpty();
        assertEquals(1, rollbackCount.get());
        assertEquals(1, closeCount.get());
    }

    private static PreparedStatementImpl prepared(long id, Disposer disposer) {
        return new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(id).build(), null, null, null, disposer);
    }

    private static String path(String string) {
        return Path.of(string).toAbsolutePath().toString();
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
}
