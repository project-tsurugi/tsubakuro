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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.framework.proto.FrameworkCommon;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.common.BlobPathMapping;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.FileBlobInfo;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.io.BlobException;

class SqlServiceStubLobWithMappingTest {

    private final MockLink link = new MockLink();

    private WireImpl wire = null;

    private Session session = null;

    private Disposer disposer = null;

    private final String ServerReceiveDirectory = "ServerReceiveDirectory";
    private final String ClientSendDirectory = "ClientSendDirectory";  
    private final String ServerSendDirectory = "ServerSendDirectory";
    private final String ClientReceiveDirectory = "ClientReceiveDirectory"; 

    private void connect(Path tempDir) {
        try {
            var ClientSendDirectoryPath = tempDir.resolve(ClientSendDirectory);
            var ServerReceiveDirectoryPath = tempDir.resolve(ServerReceiveDirectory);
            var ClientReceiveDirectoryPath = tempDir.resolve(ClientReceiveDirectory);
            var ServerSendDirectoryPath = tempDir.resolve(ServerSendDirectory);
            BlobPathMapping.Builder blobPathMappingBuilder = new BlobPathMapping.Builder();
            BlobPathMapping blobPathMapping = blobPathMappingBuilder
                                                .onSend(ClientSendDirectoryPath, ServerReceiveDirectoryPath.toString())
                                                .onReceive(ServerSendDirectoryPath.toString(), ClientReceiveDirectoryPath)
                                                .build();
            System.out.println(blobPathMapping.toString());
            wire = new WireImpl(link);
            session = new SessionImpl(false, blobPathMapping);
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
    void sendExecutePreparedStatementWithLobSuccessVirService(@TempDir Path tempDir) throws Exception {
        var ClientSendDirectoryPath = tempDir.resolve(ClientSendDirectory);
        var ServerReceiveDirectoryPath = tempDir.resolve(ServerReceiveDirectory);
        Files.createDirectory(ClientSendDirectoryPath);
        Files.createDirectory(ServerReceiveDirectoryPath);

        connect(tempDir);
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
        String fileName1 = "blob.data";
        String channelName2 = "clobChannel";
        String fileName2 = "clob.data";

        byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        Path file1 = ServerReceiveDirectoryPath.resolve(fileName1);
        Path file2 = ServerReceiveDirectoryPath.resolve(fileName2);
        Files.write(file1, data);
        Files.write(file2, data);

        Path dummy1 = ClientSendDirectoryPath.resolve(fileName1);
        Path dummy2 = ClientSendDirectoryPath.resolve(fileName2);
        Files.createFile(dummy1);
        Files.createFile(dummy2);

        var lobs = new LinkedList<FileBlobInfo>();
        lobs.add(new FileBlobInfo(channelName1, dummy1));
        lobs.add(new FileBlobInfo(channelName2, dummy2));
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
                    assertEquals(e.getPath().toString(), file1.toString());
                } else if (channelName.equals(channelName2)) {
                    assertEquals(e.getPath().toString(), file2.toString());
                } else {
                    fail("unexpected channel name " + channelName);
                }
            }
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void executeStatementWithLobVirTransaction(@TempDir Path tempDir) throws Exception {
        var ClientSendDirectoryPath = tempDir.resolve(ClientSendDirectory);
        var ServerReceiveDirectoryPath = tempDir.resolve(ServerReceiveDirectory);
        Files.createDirectory(ClientSendDirectoryPath);
        Files.createDirectory(ServerReceiveDirectoryPath);

        connect(tempDir);
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

        byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        Path file1 = ServerReceiveDirectoryPath.resolve(fileName1);
        Path file2 = ServerReceiveDirectoryPath.resolve(fileName2);
        Files.write(file1, data);
        Files.write(file2, data);

        Path dummy1 = ClientSendDirectoryPath.resolve(fileName1);
        Path dummy2 = ClientSendDirectoryPath.resolve(fileName2);
        Files.createFile(dummy1);
        Files.createFile(dummy2);
    
        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build(), null, null, null, disposer),
                                            Parameters.blobOf(parameterName1, file1),
                                            Parameters.clobOf(parameterName2, file2)).await();
                                            var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
            assertTrue(header.hasBlobs());
            for (var e: header.getBlobs().getBlobsList()) {
                String channelName = e.getChannelName();
//                String path = e.getPath();
                if (channelName.startsWith("BlobChannel-")) {
                    assertEquals(e.getPath().toString(), file1.toString());
                } else if (channelName.startsWith("ClobChannel-")) {
                    assertEquals(e.getPath().toString(), file2.toString());
                } else {
                    fail("unexpected channel name " + channelName);
                }
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void executeStatementWithLobVirTransactionNest(@TempDir Path tempDir) throws Exception {
        String childDirectory = "child";
        var ServerReceiveParentPath = tempDir.resolve(ServerReceiveDirectory);
        var ClientSendParentPath = tempDir.resolve(ClientSendDirectory);
        Files.createDirectory(ServerReceiveParentPath);
        Files.createDirectory(ClientSendParentPath);
        var ServerReceiveDirectoryPath = ServerReceiveParentPath.resolve(childDirectory);
        var ClientSendDirectoryPath = ClientSendParentPath.resolve(childDirectory);
        Files.createDirectory(ServerReceiveDirectoryPath);
        Files.createDirectory(ClientSendDirectoryPath);

        connect(tempDir);
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

        byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        Path file1 = ServerReceiveDirectoryPath.resolve(fileName1);
        Path file2 = ServerReceiveDirectoryPath.resolve(fileName2);
        Files.write(file1, data);
        Files.write(file2, data);

        Path dummy1 = ClientSendDirectoryPath.resolve(fileName1);
        Path dummy2 = ClientSendDirectoryPath.resolve(fileName2);
        Files.createFile(dummy1);
        Files.createFile(dummy2);
    
        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build(), service, null, null, disposer),
                                            Parameters.blobOf(parameterName1, file1),
                                            Parameters.clobOf(parameterName2, file2)).await();
                                            var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
            assertTrue(header.hasBlobs());
            for (var e: header.getBlobs().getBlobsList()) {
                String channelName = e.getChannelName();
//                String path = e.getPath();
                if (channelName.startsWith("BlobChannel-")) {
                    assertEquals(e.getPath().toString(), file1.toString());
                } else if (channelName.startsWith("ClobChannel-")) {
                    assertEquals(e.getPath().toString(), file2.toString());
                } else {
                    fail("unexpected channel name " + channelName);
                }
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void executeStatementWithLobWithoutFile(@TempDir Path tempDir) throws Exception {
        var ClientSendDirectoryPath = tempDir.resolve(ClientSendDirectory);
        var ServerReceiveDirectoryPath = tempDir.resolve(ServerReceiveDirectory);
        Files.createDirectory(ClientSendDirectoryPath);
        Files.createDirectory(ServerReceiveDirectoryPath);

        connect(tempDir);
        // for rollback
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());
        // for dispose transaction
        link.next(SqlResponse.Response.newBuilder().setResultOnly(SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder())).build());

        String parameterName1 = "blob";
        String fileName1 = "blob.data";
        String parameterName2 = "clob";
        String fileName2 = "clob.data";

        Path file1 = ServerReceiveDirectoryPath.resolve(fileName1);
        Path file2 = ServerReceiveDirectoryPath.resolve(fileName2);

        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            assertThrows(BlobException.class, () ->
                transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build(), service, null, null, disposer),
                                                Parameters.blobOf(parameterName1, file1),
                                                Parameters.clobOf(parameterName2, file2)));
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void getLargeObjectCache(@TempDir Path tempDir) throws Exception {
        var ServerSendDirectoryPath = tempDir.resolve(ServerSendDirectory);
        var ClientReceiveDirectoryPath = tempDir.resolve(ClientReceiveDirectory);
        Files.createDirectory(ClientReceiveDirectoryPath);

        connect(tempDir);

        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;

        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Path file = ClientReceiveDirectoryPath.resolve(fileName);
        Files.write(file, data);

        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(ServerSendDirectoryPath.resolve(fileName).toString())))
                        .build();
        var payload = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                                   .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder()
                                                                .setChannelName(channelName)))
                        .build();
        // for getLargeObjectData
        link.next(header, payload);
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
            var largeObjectCache = transaction.getLargeObjectCache(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), objectId)).await();
            var pathOpt = largeObjectCache.find();
            assertTrue(pathOpt.isPresent());
            var obtainedData = Files.readAllBytes(pathOpt.get());
            assertEquals(data.length, obtainedData.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], obtainedData[i]);
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void getLargeObjectCacheNest(@TempDir Path tempDir) throws Exception {
        String childDirectory = "child";
        var ServerSendParentPath = tempDir.resolve(ServerSendDirectory);
        var ClientReceiveParentPath = tempDir.resolve(ClientReceiveDirectory);
        Files.createDirectory(ClientReceiveParentPath);
        var ServerSendDirectoryPath = ServerSendParentPath.resolve(childDirectory);
        var ClientReceiveDirectoryPath = ClientReceiveParentPath.resolve(childDirectory);
        Files.createDirectory(ClientReceiveDirectoryPath);

        connect(tempDir);

        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;

        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Path file = ClientReceiveDirectoryPath.resolve(fileName);
        Files.write(file, data);

        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(ServerSendDirectoryPath.resolve(fileName).toString())))
                        .build();
        var payload = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                                   .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder()
                                                                .setChannelName(channelName)))
                        .build();
        // for getLargeObjectData
        link.next(header, payload);
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
            var largeObjectCache = transaction.getLargeObjectCache(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), objectId)).await();
            var pathOpt = largeObjectCache.find();
            assertTrue(pathOpt.isPresent());
            var obtainedData = Files.readAllBytes(pathOpt.get());
            assertEquals(data.length, obtainedData.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], obtainedData[i]);
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void getLargeObjectCacheNest_noConvert(@TempDir Path tempDir) throws Exception {
        String childDirectory = "data/log/blob";
        var ServerSendParentPath = tempDir.resolve("mnt/cygwin64/tmp");
        var ClientReceiveParentPath = tempDir.resolve("cygwin64/tmp");
        var ServerSendDirectoryPath = ServerSendParentPath.resolve(childDirectory);
        var ClientReceiveDirectoryPath = ClientReceiveParentPath.resolve(childDirectory);
        Files.createDirectories(ServerSendDirectoryPath);
        Files.createDirectories(ClientReceiveDirectoryPath);

        try {
            var ServerMappingParentPath = tempDir.resolve("opt/tsurugi/var");
            BlobPathMapping.Builder blobPathMappingBuilder = new BlobPathMapping.Builder();
            BlobPathMapping blobPathMapping = blobPathMappingBuilder
                                                .onReceive(ServerMappingParentPath.toString(), ClientReceiveParentPath)
                                                .build();
            System.out.println(blobPathMapping.toString());
            wire = new WireImpl(link);
            session = new SessionImpl(false, blobPathMapping);
            disposer = ((SessionImpl) session).disposer();
            session.connect(wire);
        } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }

        String fileName = "0000000000000002.blob";
        String channelName = "lobChannel";
        long objectId = 12345;

        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Path file = ServerSendDirectoryPath.resolve(fileName);
        Files.write(file, data);

        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(ServerSendDirectoryPath.resolve(fileName).toString())))
                        .build();
        var payload = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                                   .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder()
                                                                .setChannelName(channelName)))
                        .build();
        // for getLargeObjectData
        link.next(header, payload);
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
            var largeObjectCache = transaction.getLargeObjectCache(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), objectId)).await();
            var pathOpt = largeObjectCache.find();
            assertTrue(pathOpt.isPresent());
            var obtainedData = Files.readAllBytes(pathOpt.get());
            assertEquals(data.length, obtainedData.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], obtainedData[i]);
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void getLargeObjectCacheNoFile(@TempDir Path tempDir) throws Exception {
        var ServerSendDirectoryPath = tempDir.resolve(ServerSendDirectory);
        var ClientReceiveDirectoryPath = tempDir.resolve(ClientReceiveDirectory);
        Files.createDirectory(ServerSendDirectoryPath);
        Files.createDirectory(ClientReceiveDirectoryPath);

        connect(tempDir);

        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;

        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Path file = ClientReceiveDirectoryPath.resolve(fileName);

        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(ServerSendDirectoryPath.resolve(fileName).toString())))
                        .build();
        var payload = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                                   .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder()
                                                                .setChannelName(channelName)))
                        .build();
        // for getLargeObjectData
        link.next(header, payload);
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
            var largeObjectCache = transaction.getLargeObjectCache(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), objectId)).await();
            var pathOpt = largeObjectCache.find();
            assertFalse(pathOpt.isPresent());
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void copyToNoFile(@TempDir Path tempDir) throws Exception {
        var ServerSendDirectoryPath = tempDir.resolve(ServerSendDirectory);
        var ClientReceiveDirectoryPath = tempDir.resolve(ClientReceiveDirectory);
        Files.createDirectory(ServerSendDirectoryPath);
        Files.createDirectory(ClientReceiveDirectoryPath);

        connect(tempDir);

        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;
        Path file = ClientReceiveDirectoryPath.resolve(fileName);

        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(ServerSendDirectoryPath.resolve(fileName).toString())))
                        .build();
        var payload = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                                   .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder()
                                                                .setChannelName(channelName)))
                        .build();
        // for getLargeObjectData
        link.next(header, payload);
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
            Path copy = tempDir.resolve("lob_copy.data");
            assertThrows(BlobException.class, () ->
                transaction.copyTo(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), objectId), copy).await());
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void copyTo(@TempDir Path tempDir) throws Exception {
        var ServerSendDirectoryPath = tempDir.resolve(ServerSendDirectory);
        var ClientReceiveDirectoryPath = tempDir.resolve(ClientReceiveDirectory);
        Files.createDirectory(ServerSendDirectoryPath);
        Files.createDirectory(ClientReceiveDirectoryPath);

        connect(tempDir);

        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;

        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Path file = ClientReceiveDirectoryPath.resolve("lob.data");
        Files.write(file, data);

        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(ServerSendDirectoryPath.resolve(fileName).toString())))
                        .build();
        var payload = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                                   .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder()
                                                                .setChannelName(channelName)))
                        .build();
        // for getLargeObjectData
        link.next(header, payload);
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
            Path copy = tempDir.resolve("lob_copy.data");
            var largeObjectCache = transaction.copyTo(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), objectId), copy).await();
            var obtainedData = Files.readAllBytes(copy);
            assertTrue(Files.exists(copy));
            assertEquals(data.length, obtainedData.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], obtainedData[i]);
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());
    }

    @Test
    void executeStatementWithLobVirTransactionRelativePath(@TempDir Path directory) throws Exception {
        System.setProperty("user.dir", directory.toString());

        String rootDirectory = "relative_path_send_test";
        var rootDirectoryPath = Paths.get(rootDirectory);
        FileDeleter.deleteFiles(new File(rootDirectoryPath.toAbsolutePath().toString()));
        var tempDir = Files.createDirectory(rootDirectoryPath.toAbsolutePath());

        var ClientSendDirectoryPath = rootDirectoryPath.resolve(ClientSendDirectory);
        var ServerReceiveDirectoryPath = tempDir.resolve(ServerReceiveDirectory).toAbsolutePath();
        Files.createDirectory(ClientSendDirectoryPath);
        Files.createDirectory(ServerReceiveDirectoryPath);

        try {
            BlobPathMapping.Builder blobPathMappingBuilder = new BlobPathMapping.Builder();
            BlobPathMapping blobPathMapping = blobPathMappingBuilder
                                                .onSend(ClientSendDirectoryPath, ServerReceiveDirectoryPath.toString())
                                                .build();
            System.out.println(blobPathMapping.toString());
            wire = new WireImpl(link);
            session = new SessionImpl(false, blobPathMapping);
            disposer = ((SessionImpl) session).disposer();
            session.connect(wire);
        } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }

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

        byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        Path file1 = ServerReceiveDirectoryPath.resolve(fileName1);
        Path file2 = ServerReceiveDirectoryPath.resolve(fileName2);
        Files.write(file1, data);
        Files.write(file2, data);

        Path dummy1 = ClientSendDirectoryPath.resolve(fileName1);
        Path dummy2 = ClientSendDirectoryPath.resolve(fileName2);
        Files.createFile(dummy1);
        Files.createFile(dummy2);
    
        try (
            var service = new SqlServiceStub(session);
            var transaction = new TransactionImpl(SqlResponse.Begin.Success.newBuilder()
                                              .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(123).build())
                                              .build(),
                                              service,
                                              null,
                                              disposer);
        ) {
            transaction.executeStatement(new PreparedStatementImpl(SqlCommon.PreparedStatement.newBuilder().setHandle(456).build(), service, null, null, disposer),
                                            Parameters.blobOf(parameterName1, file1),
                                            Parameters.clobOf(parameterName2, file2)).await();
                                            var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
            assertTrue(header.hasBlobs());
            for (var e: header.getBlobs().getBlobsList()) {
                String channelName = e.getChannelName();
//                String path = e.getPath();
                if (channelName.startsWith("BlobChannel-")) {
                    assertEquals(e.getPath().toString(), file1.toString());
                } else if (channelName.startsWith("ClobChannel-")) {
                    assertEquals(e.getPath().toString(), file2.toString());
                } else {
                    fail("unexpected channel name " + channelName);
                }
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());

        FileDeleter.deleteFiles(new File(rootDirectoryPath.toAbsolutePath().toString()));
    }

    @Test
    void getLargeObjectCacheRelativePath(@TempDir Path directory) throws Exception {
        System.setProperty("user.dir", directory.toString());

        String rootDirectory = "relative_path_receive_test";
        var rootDirectoryPath = Paths.get(rootDirectory);
        FileDeleter.deleteFiles(new File(rootDirectoryPath.toAbsolutePath().toString()));
        var tempDir = Files.createDirectory(rootDirectoryPath.toAbsolutePath());

        var ClientReceiveDirectoryPath = rootDirectoryPath.resolve(ClientReceiveDirectory);
        var ServerSendDirectoryPath = tempDir.resolve(ServerSendDirectory).toAbsolutePath();
        Files.createDirectory(ClientReceiveDirectoryPath);
        Files.createDirectory(ServerSendDirectoryPath);

        try {
            BlobPathMapping.Builder blobPathMappingBuilder = new BlobPathMapping.Builder();
            BlobPathMapping blobPathMapping = blobPathMappingBuilder
                                                .onReceive(ServerSendDirectoryPath.toString(), ClientReceiveDirectoryPath)
                                                .build();
            System.out.println(blobPathMapping.toString());
            wire = new WireImpl(link);
            session = new SessionImpl(false, blobPathMapping);
            disposer = ((SessionImpl) session).disposer();
            session.connect(wire);
        } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }

        String fileName = "lob.data";
        String channelName = "lobChannel";
        long objectId = 12345;

        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Path file = ClientReceiveDirectoryPath.resolve(fileName);
        Files.write(file, data);

        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(ServerSendDirectoryPath.resolve(fileName).toString())))
                        .build();
        var payload = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                                   .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder()
                                                                .setChannelName(channelName)))
                        .build();
        // for getLargeObjectData
        link.next(header, payload);
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
            var largeObjectCache = transaction.getLargeObjectCache(new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), objectId)).await();
            var pathOpt = largeObjectCache.find();
            assertTrue(pathOpt.isPresent());
            var obtainedData = Files.readAllBytes(pathOpt.get());
            assertEquals(data.length, obtainedData.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], obtainedData[i]);
            }
        }
        disposer.waitForEmpty();
        assertFalse(link.hasRemaining());

        FileDeleter.deleteFiles(new File(rootDirectoryPath.toAbsolutePath().toString()));
    }

    static private class FileDeleter {
        public static void deleteFiles(File dir) {        
            if(dir.exists()) {
                if(dir.isDirectory()) {
                    File[] files = dir.listFiles();
                    if(files != null) {
                        for(int i=0; i<files.length; i++) {
                            var tf = files[i];
                            if(!tf.exists()) {
                                continue;
                            } else if(tf.isFile()) {
                                tf.delete();
                            } else if(tf.isDirectory()) {
                                FileDeleter.deleteFiles(tf);
                                tf.delete();
                            }        
                        }
                    }
                }
                dir.delete();
            }
        }
    }

    @Test
    void relativeServerPath_onSend(@TempDir Path tempDir) throws Exception {
        BlobPathMapping.Builder blobPathMappingBuilder = new BlobPathMapping.Builder();
        BlobPathMapping blobPathMapping = blobPathMappingBuilder
                                            .onSend(tempDir, "server_relative_directory")
                                            .onReceive("/server_relative_directory", tempDir)
                                            .build();
        
        wire = new WireImpl(link);
        var e = assertThrows(IllegalArgumentException.class, () -> session = new SessionImpl(false, blobPathMapping));
        assertEquals("server path must be absolute", e.getMessage());
    }

    @Test
    void relativeServerPath_onReceive(@TempDir Path tempDir) throws Exception {
        BlobPathMapping.Builder blobPathMappingBuilder = new BlobPathMapping.Builder();
        BlobPathMapping blobPathMapping = blobPathMappingBuilder
                                            .onSend(tempDir, "/server_relative_directory")
                                            .onReceive("server_relative_directory", tempDir)
                                            .build();
        
        wire = new WireImpl(link);
        var e = assertThrows(IllegalArgumentException.class, () -> session = new SessionImpl(false, blobPathMapping));
        assertEquals("server path must be absolute", e.getMessage());
    }
}
