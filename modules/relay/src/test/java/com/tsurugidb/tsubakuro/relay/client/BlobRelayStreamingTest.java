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
package com.tsurugidb.tsubakuro.relay.client;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.blob_relay.proto.BlobRelayStreamingGrpc;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.relay.server.BlobRelayStreamingServer;

class BlobRelayStreamingTest {
    private static final Logger LOG = LoggerFactory.getLogger(BlobRelayStreamingTest.class);
    
    private static final int TEST_DATA_SIZE = 1024 * 2 + 123; // 2KB + 123 bytes to ensure multiple chunks are tested

    private BlobRelayStreamingServer server;
    private BlobRelayStreaming client;

    @BeforeAll
    static void beforeAll() {
        LOG.info("BlobRelayStreamingTest start");
    }
    @BeforeEach
    void startup() {
        try {
            server = new BlobRelayStreamingServer();
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @AfterEach
    void teardown() throws IOException, InterruptedException {
        server.stop();
        if (client != null) {
            client.close();
        }
    }

    @Test
    void put() throws Exception {
        // prepare server responses
        var response = Streaming.PutStreamingResponse.newBuilder()
                                            .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                                .setStorageId(1)
                                                .setObjectId(23)
                                                .setTag(45)
                                                .build())
                                            .build();
        server.addPutResponse(response);

        // test put() method
        client = new BlobRelayStreaming("localhost:" + server.getPort(), false, 1024);
        var data = new byte[TEST_DATA_SIZE];
        new Random().nextBytes(data);
        var future = client.put(Streaming.PutStreamingRequest.Metadata.newBuilder()
                                                                            .setSessionId(128)
                                                                      .build(),
                                new ByteArrayInputStream(data));
        var result = future.get();

        // verify received data and response
        assertNotNull(result);
        assertEquals(response.getBlob(), result);
        assertArrayEquals(data, server.receivedData());
    }

    @Test
    void putTimeout() throws Exception {
        server.injectFault(BlobRelayStreamingServer.FaultType.NoResponse);

        // test put() method
        client = new BlobRelayStreaming("localhost:" + server.getPort(), false, 1024);
        var data = new byte[TEST_DATA_SIZE];
        new Random().nextBytes(data);

        try (var future = client.put(Streaming.PutStreamingRequest.Metadata.newBuilder()
                                                                                .setSessionId(128)
                                                                            .build(),
                                     new ByteArrayInputStream(data))) {
            assertThrows(TimeoutException.class, () -> {
                future.get(1, TimeUnit.SECONDS);
            });
        }
    }

    @Test
    void get() throws Exception {
        // prepare server responses
        final int blobSize = 1024 * 10;

        var buffer = new byte[blobSize];
        new java.util.Random().nextBytes(buffer);
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                                .setChunk(com.google.protobuf.ByteString.copyFrom(buffer))
                                                            .build());
        server.addGetResponse(Streaming.GetStreamingResponse.newBuilder()
                                                                .setMetadata(Streaming.GetStreamingResponse.Metadata.newBuilder()
                                                                .setBlobSize(blobSize)
                                                                .build())
                                                            .build());

        // test get() method
        client = new BlobRelayStreaming("localhost:" + server.getPort(), false, 1024);  // Use a smaller chunk size to test multiple chunks
        var inputStream = client.get(Streaming.GetStreamingRequest.newBuilder()
                                                    .setTransactionId(789)
                                                    .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                                        .setStorageId(1)
                                                        .setObjectId(23)
                                                        .setTag(45))
                                                .build());
        var data = inputStream.get().readAllBytes();

        // verify received data
        assertArrayEquals(buffer, data);
    }

    @Test
    void getTimeout() throws Exception {
        server.injectFault(BlobRelayStreamingServer.FaultType.NoResponse);

        // test get() method
        client = new BlobRelayStreaming("localhost:" + server.getPort(), false, 1024);

        var inputStream = client.get(Streaming.GetStreamingRequest.newBuilder()
                                                .setTransactionId(789)
                                                .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                                    .setStorageId(1)
                                                    .setObjectId(23)
                                                    .setTag(45))
                                            .build())
                                .get(1, TimeUnit.SECONDS);
        Throwable exception = assertThrows(ResponseTimeoutException.class, () -> {
            inputStream.readAllBytes();
        });
    }

    @Test
    void getTimeoutWithPath(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("blob_data.bin");

        server.injectFault(BlobRelayStreamingServer.FaultType.NoResponse);

        // test get() method
        client = new BlobRelayStreaming("localhost:" + server.getPort(), false, 1024);
        try (var future = client.get(Streaming.GetStreamingRequest.newBuilder()
                                .setTransactionId(789)
                                .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                    .setStorageId(1)
                                    .setObjectId(23)
                                    .setTag(45))
                            .build(),
                            file)) {
            var e =assertThrows(TimeoutException.class, () -> {
                future.get(1, TimeUnit.SECONDS);
            });
        }
        assertTrue(Files.notExists(file), "File should not be created on timeout");
    }
}