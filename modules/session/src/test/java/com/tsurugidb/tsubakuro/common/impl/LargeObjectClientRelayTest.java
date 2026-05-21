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
package com.tsurugidb.tsubakuro.common.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
// import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
// import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.FileInputStream;
import java.io.Reader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.tsurugidb.blob_relay.proto.BlobRelayStreamingGrpc;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;
import com.tsurugidb.core.proto.CoreRequest;
import com.tsurugidb.core.proto.CoreResponse;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.common.LargeObjectReference;
import com.tsurugidb.tsubakuro.relay.server.BlobRelayStreamingServer;

class LargeObjectClientRelayTest {
    private static final int SERVER_PORT = 65521;

    private LargeObjectClient client = null;

    private BlobRelayStreamingServer server = null;

    private final byte[] data;

    private final LargeObjectClient.ContextId contextId;

    private final LargeObjectReference lobReference;

    public LargeObjectClientRelayTest() {
        this.data = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '\n' };

        contextId = new LargeObjectClient.ContextId() {
            @Override
            public ContextIdKind contextIdKind() {
                return ContextIdKind.TRANSACTION;
            }

            @Override
            public long getTransactionHandle() {
                return 123;
            }
        };
        lobReference = new LargeObjectReference() {
            @Override
            public long getProvider() {
                return 1;
            }

            @Override
            public long getObjectId() {
                return 12345;
            }

            @Override
            public long getReferenceTag() {
                return 678;
            }
        };
    }

    @BeforeEach
    void startup() {
        try {
            server = new BlobRelayStreamingServer(SERVER_PORT);
            server.start();

            client = new LargeObjectClientRelay("123", "localhost:" + SERVER_PORT, false, 1024 * 1024);
        } catch (IOException e) {
            System.err.println(e);
            e.printStackTrace();
            fail("failed to start server");
        }
    }
    @AfterEach
    void tearDown() throws Exception {
        client.close();
        server.stop();
    }

    @Test
    void upload_path(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("blob.data");
        Files.write(dataFile, data);

        // for blob relay service
        server.addPutResponse(Streaming.PutStreamingResponse.newBuilder()
                                .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                    .setStorageId(1)
                                    .setObjectId(23)
                                    .setTag(45))
                                .build());

          var ref = client.upload(dataFile).await();
        assertNotNull(ref);
        assertEquals(LargeObjectInfo.InfoType.BLOB_RELAY_REFERENCE, ref.getInfoType());
        var blobRef = ref.getBlobRelayReference();
        assertEquals(1, blobRef.getStorageId());
        assertEquals(23, blobRef.getObjectId());
        assertEquals(45, blobRef.getReferenceTag());

        assertArrayEquals(data, server.receivedData());
        assertFalse(server.hasRemaining());
    }

    @Test
    void upload_input_stream(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("blob.data");
        Files.write(dataFile, data);
        FileInputStream fis = new FileInputStream(dataFile.toFile());

        // for blob relay service
        server.addPutResponse(Streaming.PutStreamingResponse.newBuilder()
                                .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                    .setStorageId(1)
                                    .setObjectId(23)
                                    .setTag(45))
                                .build());

        var ref = client.upload(fis).await();
        assertNotNull(ref);
        assertEquals(LargeObjectInfo.InfoType.BLOB_RELAY_REFERENCE, ref.getInfoType());
        var blobRef = ref.getBlobRelayReference();
        assertEquals(1, blobRef.getStorageId());
        assertEquals(23, blobRef.getObjectId());
        assertEquals(45, blobRef.getReferenceTag());

        assertArrayEquals(data, server.receivedData());
        assertFalse(server.hasRemaining());
    }

    @Test
    void upload_reader(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("clob.data");
        Files.write(dataFile, data);
        FileInputStream fis = new FileInputStream(dataFile.toFile());
        Reader reader = new java.io.InputStreamReader(fis);

        // for blob relay service
        server.addPutResponse(Streaming.PutStreamingResponse.newBuilder()
                                .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                    .setStorageId(1)
                                    .setObjectId(23)
                                    .setTag(45))
                                .build());

        var ref = client.upload(reader).await();
        assertNotNull(ref);
        assertEquals(LargeObjectInfo.InfoType.BLOB_RELAY_REFERENCE, ref.getInfoType());
        var blobRef = ref.getBlobRelayReference();
        assertEquals(1, blobRef.getStorageId());
        assertEquals(23, blobRef.getObjectId());
        assertEquals(45, blobRef.getReferenceTag());

        assertArrayEquals(data, server.receivedData());
        assertFalse(server.hasRemaining());
    }

    @Test
    void openInputStream() throws Exception {
        // for blob relay service
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                .setChunk(com.google.protobuf.ByteString.copyFrom(data))
                                .build());
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                .setMetadata(Streaming.GetStreamingResponse.Metadata.newBuilder()
                                    .setBlobSize(data.length))
                                .build());

        var inputStream = client.openInputStream(contextId, lobReference).await();

        assertNotNull(inputStream);
        var obtainedData = inputStream.readAllBytes();
        assertArrayEquals(data, obtainedData);

        var lobReference = server.getBlobReference();
        assertEquals(1, lobReference.getStorageId());
        assertEquals(12345, lobReference.getObjectId());
        assertEquals(678, lobReference.getTag());
        assertFalse(server.hasRemaining());
    }

    @Test
    void openReader() throws Exception {
        // for blob relay service
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                .setChunk(com.google.protobuf.ByteString.copyFrom(data))
                                .build());
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                .setMetadata(Streaming.GetStreamingResponse.Metadata.newBuilder()
                                    .setBlobSize(data.length))
                                .build());

        var reader = client.openReader(contextId, lobReference).await();

        assertNotNull(reader);
        char[] obtainedData = new char[data.length];
        reader.read(obtainedData);
        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], obtainedData[i]);
        }
        assertEquals(-1, reader.read());

        var lobReference = server.getBlobReference();
        assertEquals(1, lobReference.getStorageId());
        assertEquals(12345, lobReference.getObjectId());
        assertEquals(678, lobReference.getTag());
        assertFalse(server.hasRemaining());
    }

    
    @Test
    void copyTo(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("blob.data");

        // for blob relay service
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                .setChunk(com.google.protobuf.ByteString.copyFrom(data))
                                .build());
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                .setMetadata(Streaming.GetStreamingResponse.Metadata.newBuilder()
                                    .setBlobSize(data.length))
                                .build());

        var reader = client.copyTo(contextId, lobReference, file).await();

        assertTrue(Files.exists(file));
        var obtainedData = Files.readAllBytes(file);
        assertArrayEquals(data, obtainedData);

        var lobReference = server.getBlobReference();
        assertEquals(1, lobReference.getStorageId());
        assertEquals(12345, lobReference.getObjectId());
        assertEquals(678, lobReference.getTag());
        assertFalse(server.hasRemaining());
    }
}