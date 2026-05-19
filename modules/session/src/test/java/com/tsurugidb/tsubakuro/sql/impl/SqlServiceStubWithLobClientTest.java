/*
 * Copyright 2023-2026 Project Tsurugi.
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.blob_relay.proto.BlobRelayStreamingGrpc;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;
import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.framework.proto.FrameworkCommon;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.BlobInfoImpl;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.grpc.server.BlobRelayStreamingServer;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.common.exception.BlobException;

class SqlServiceStubWithLobClientTest {
    private static final int SERVER_PORT = 65432;

    private final MockLink link = new MockLink();

    private WireImpl wire = null;

    private Session session = null;

    private Disposer disposer = null;

    private LargeObjectClient largeObjectClient = null;

    private BlobRelayStreamingServer server = null;

    private byte[] data = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                       'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '\n' };

    @BeforeEach
    void startup() {
        try {
            server = new BlobRelayStreamingServer(SERVER_PORT);
            server.start();

            wire = new WireImpl(link);
            session = new SessionImpl();
            disposer = ((SessionImpl) session).disposer();
            link.next(EndpointResponse.Handshake.newBuilder()
                                                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                                                        .setSessionId(123)
                                                        .setBlobRelayServiceInfo(EndpointResponse.BlobRelayServiceInfo.newBuilder()
                                                            .setBlobSessionId(246)
                                                            .setEndpoint("dns:///localhost:" + SERVER_PORT)
                                                            .setMedium("stream")))
                                                .build());
            long sessionId = wire.handshake(new ClientInformation(), null, 0, null).get();
            session.connect(wire);
            largeObjectClient = session.getLargeObjectClient();
        } catch (IOException | InterruptedException | ServerException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        if (session != null) {
            session.close();
        }
        if (largeObjectClient != null) {
            largeObjectClient.close();
        }
        if (server != null) {
            server.stop();
            server.blockUntilShutdown();
        }
    }

    @Test
    void executeStatementWithLob(@TempDir Path tempDir) throws Exception {
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
        String fileName1 = "blob.data";
        String parameterName2 = "clob";
        String fileName2 = "clob.data";

        Path file1 = tempDir.resolve(fileName1);
        Path file2 = tempDir.resolve(fileName2);
        Files.write(file1, data);
        Files.write(file2, data);

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer,
                                              session.getLargeObjectClient());
        ) {
            // for blob relay service
            server.addPutResponse(Streaming.PutStreamingResponse.newBuilder()
                                    .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                        .setStorageId(1)
                                        .setObjectId(23)
                                        .setTag(45)
                                        .build())
                                    .build());
            server.addPutResponse(Streaming.PutStreamingResponse.newBuilder()
                                    .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                        .setStorageId(1)
                                        .setObjectId(35)
                                        .setTag(57)
                                        .build())
                                    .build());

            transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build(), service, null, null, disposer),
                                            Parameters.blobOf(parameterName1, largeObjectClient.upload(file1).get()),
                                            Parameters.clobOf(parameterName2, largeObjectClient.upload(file2).get())).await();
            var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
            assertTrue(header.hasBlobs());
            for (var e: header.getBlobs().getBlobsList()) {
                String channelName = e.getChannelName();
                assertEquals(e.getBlobLocationCase(), FrameworkCommon.BlobInfo.BlobLocationCase.BLOB);
                if (channelName.startsWith("BlobChannel-")) {
                    var blob = e.getBlob();
                    assertEquals(blob.getStorageId(), 1);
                    assertEquals(blob.getObjectId(), 23);
                    assertEquals(blob.getTag(), 45);
                } else if (e.getBlob().getObjectId() == 35) {
                    var blob = e.getBlob();
                    assertEquals(blob.getStorageId(), 1);
                    assertEquals(blob.getObjectId(), 35);
                    assertEquals(blob.getTag(), 57);
                } else {
                    fail("unexpected object id " + e.getBlob().getObjectId());
                }
            }
            assertArrayEquals(data, server.receivedData(0));
            assertArrayEquals(data, server.receivedData(-1));
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
        assertFalse(server.hasRemaining());
    }

    @Test
    void executeStatementWithLobWithoutFile(@TempDir Path tempDir) throws Exception {
        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        String parameterName1 = "blob";
        String fileName1 = "blob.data";
        String parameterName2 = "clob";
        String fileName2 = "clob.data";

        Path file1 = tempDir.resolve(fileName1);
        Path file2 = tempDir.resolve(fileName2);

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer,
                                              session.getLargeObjectClient());
        ) {
            assertThrows(BlobException.class, () ->
                transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build(), service, null, null, disposer),
                                                Parameters.blobOf(parameterName1, largeObjectClient.upload(file1).get()),
                                                Parameters.clobOf(parameterName2, largeObjectClient.upload(file2).get())).await());
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void executeStatementWithLobStream(@TempDir Path tempDir) throws Exception {
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
        String fileName1 = "blob.data";
        String parameterName2 = "clob";
        String fileName2 = "clob.data";

        Path file1 = tempDir.resolve(fileName1);
        Path file2 = tempDir.resolve(fileName2);
        Files.write(file1, data);
        Files.write(file2, data);

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer,
                                              session.getLargeObjectClient());
        ) {
            // for blob relay service
            server.addPutResponse(Streaming.PutStreamingResponse.newBuilder()
                                    .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                        .setStorageId(1)
                                        .setObjectId(23)
                                        .setTag(45)
                                        .build())
                                    .build());
            server.addPutResponse(Streaming.PutStreamingResponse.newBuilder()
                                    .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                        .setStorageId(1)
                                        .setObjectId(35)
                                        .setTag(57)
                                        .build())
                                    .build());

            transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build(), service, null, null, disposer),
                                            Parameters.blobOf(parameterName1, largeObjectClient.upload(new FileInputStream(file1.toFile())).get()),
                                            Parameters.clobOf(parameterName2, largeObjectClient.upload(new InputStreamReader(new FileInputStream(file2.toFile()))).get())).await();
            var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
            assertTrue(header.hasBlobs());
            for (var e: header.getBlobs().getBlobsList()) {
                String channelName = e.getChannelName();
                assertEquals(e.getBlobLocationCase(), FrameworkCommon.BlobInfo.BlobLocationCase.BLOB);
                if (channelName.startsWith("BlobChannel-")) {
                    var blob = e.getBlob();
                    assertEquals(blob.getStorageId(), 1);
                    assertEquals(blob.getObjectId(), 23);
                    assertEquals(blob.getTag(), 45);
                } else if (e.getBlob().getObjectId() == 35) {
                    var blob = e.getBlob();
                    assertEquals(blob.getStorageId(), 1);
                    assertEquals(blob.getObjectId(), 35);
                    assertEquals(blob.getTag(), 57);
                } else {
                    fail("unexpected object id " + e.getBlob().getObjectId());
                }
            }
            assertArrayEquals(data, server.receivedData(0));
            assertArrayEquals(data, server.receivedData(-1));
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
        assertFalse(server.hasRemaining());
    }

    @Test
    void openInputStream(@TempDir Path tempDir) throws Exception {
        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;
        long referenceTag = 678;

        Path file = tempDir.resolve("lob.data");
        Files.write(file, data);

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
                                              disposer,
                                              session.getLargeObjectClient());
        ) {
            server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                        .setChunk(com.google.protobuf.ByteString.copyFrom(data))
                                                    .build());
            server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                                    .setMetadata(Streaming.GetStreamingResponse.Metadata.newBuilder()
                                                                        .setBlobSize(data.length))
                                                                    .build());

            var inputStream = transaction.openInputStream(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(1), objectId, referenceTag)).await();
            var obtainedData = inputStream.readAllBytes();
            assertArrayEquals(data, obtainedData);
        }
        var lobReference = server.getBlobReference();
        assertEquals(1, lobReference.getStorageId());
        assertEquals(12345, lobReference.getObjectId());
        assertEquals(678, lobReference.getTag());
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void openReader(@TempDir Path tempDir) throws Exception {
        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;
        long referenceTag = 678;

        Path file = tempDir.resolve("lob.data");
        Files.write(file, data);

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
                                              disposer,
                                              session.getLargeObjectClient());
        ) {
            server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                        .setChunk(com.google.protobuf.ByteString.copyFrom(data))
                                                    .build());
            server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                                    .setMetadata(Streaming.GetStreamingResponse.Metadata.newBuilder()
                                                                        .setBlobSize(data.length))
                                                                    .build());

            var reader = transaction.openReader(new ClobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(1), objectId, referenceTag)).await();
            char[] obtainedData = new char[data.length];
            reader.read(obtainedData);
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], obtainedData[i]);
            }
            assertEquals(-1, reader.read());
        }
        var lobReference = server.getBlobReference();
        assertEquals(1, lobReference.getStorageId());
        assertEquals(12345, lobReference.getObjectId());
        assertEquals(678, lobReference.getTag());
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void getLargeObjectCache(@TempDir Path tempDir) throws Exception {
        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;
        long referenceTag = 678;

        Path file = tempDir.resolve(fileName);
        Files.write(file, data);

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
                                              disposer,
                                              session.getLargeObjectClient());
        ) {
            var largeObjectCache = transaction.getLargeObjectCache(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(1), objectId, referenceTag)).await();
            var pathOpt = largeObjectCache.find();
            assertFalse(pathOpt.isPresent());
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void copyTo(@TempDir Path tempDir) throws Exception {
        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;
        long referenceTag = 678;

        Path file = tempDir.resolve("lob.data");
        Files.write(file, data);

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
                                              disposer,
                                              session.getLargeObjectClient());
        ) {
            server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                        .setChunk(com.google.protobuf.ByteString.copyFrom(data))
                                                    .build());
            server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                                    .setMetadata(Streaming.GetStreamingResponse.Metadata.newBuilder()
                                                                        .setBlobSize(data.length))
                                                                    .build());

            Path copy = tempDir.resolve("lob_copy.data");
            var largeObjectCache = transaction.copyTo(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(1), objectId, referenceTag), copy).await();
            var obtainedData = Files.readAllBytes(copy);
            assertTrue(Files.exists(copy));
            assertEquals(data.length, obtainedData.length);
            assertArrayEquals(data, obtainedData);
        }
        var lobReference = server.getBlobReference();
        assertEquals(1, lobReference.getStorageId());
        assertEquals(12345, lobReference.getObjectId());
        assertEquals(678, lobReference.getTag());
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }
}