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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.FileBlobInfo;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.SqlService;

class SqlServiceStubLobTest {

    private final MockLink link = new MockLink();

    private WireImpl wire = null;

    private Session session = null;

    SqlServiceStubLobTest() {
        try {
            wire = new WireImpl(link);
            session = new SessionImpl(wire);
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
    void sendExecutePreparedStatementWithLobSuccess() throws Exception {
        link.next(SqlResponse.Response.newBuilder()
                    .setExecuteResult(SqlResponse.ExecuteResult.newBuilder()
                                        .setSuccess(SqlResponse.ExecuteResult.Success.newBuilder()
                                                        .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.INSERTED_ROWS).setValue(1))
                                                        .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.UPDATED_ROWS).setValue(2))
                                                        .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.MERGED_ROWS).setValue(3))
                                                        .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.DELETED_ROWS).setValue(4))
                                        )
                    ).build()
        );

        var message = SqlRequest.ExecutePreparedStatement.newBuilder()
                .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(12345).build())
                .build();

        String channelName1 = "blobChannel";
        String fileName1 = "/tmp/blob.data";
        String channelName2 = "clobChannel";
        String fileName2 = "/tmp/clob.data";
        var lobs = new LinkedList<FileBlobInfo>();
        lobs.add(new FileBlobInfo(channelName1, Path.of(fileName1)));
        lobs.add(new FileBlobInfo(channelName2, Path.of(fileName2)));
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

            var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
            assertTrue(header.hasBlobs());
            for (var e: header.getBlobs().getBlobsList()) {
                String channelName = e.getChannelName();
                String path = e.getPath();
                if (channelName.equals(channelName1)) {
                    assertEquals(e.getPath(), fileName1);
                } else if (channelName.equals(channelName2)) {
                    assertEquals(e.getPath(), fileName2);
                } else {
                    fail("unexpected channel name " + channelName);
                }
            }
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void executeStatementWithLob() throws Exception {
        // for executeStatement
        link.next(SqlResponse.Response.newBuilder()
                    .setExecuteResult(SqlResponse.ExecuteResult.newBuilder()
                            .setSuccess(SqlResponse.ExecuteResult.Success.newBuilder()
                                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.INSERTED_ROWS).setValue(1))
                                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.UPDATED_ROWS).setValue(2))
                                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.MERGED_ROWS).setValue(3))
                                            .addCounters(SqlResponse.ExecuteResult.CounterEntry.newBuilder().setType(SqlResponse.ExecuteResult.CounterType.DELETED_ROWS).setValue(4))
                            )
                    ).build()
        );
        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        String parameterName1 = "blob";
        String fileName1 = "/tmp/blob.data";
        String parameterName2 = "clob";
        String fileName2 = "/tmp/clob.data";

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null);
        ) {
            transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build()),
                                            Parameters.blobOf(parameterName1, Paths.get(fileName1)),
                                            Parameters.clobOf(parameterName2, Paths.get(fileName2))).await();
                                            var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
            assertTrue(header.hasBlobs());
            for (var e: header.getBlobs().getBlobsList()) {
                String channelName = e.getChannelName();
                String path = e.getPath();
                if (channelName.startsWith("BlobChannel-")) {
                    assertEquals(e.getPath(), fileName1);
                } else if (channelName.startsWith("ClobChannel-")) {
                    assertEquals(e.getPath(), fileName2);
                } else {
                    fail("unexpected channel name " + channelName);
                }
            }
        }
        assertFalse(link.hasRemaining());
    }
}
