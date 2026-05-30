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
import java.util.Random;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.blob_relay.proto.BlobRelayStreamingGrpc;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;
import com.tsurugidb.tsubakuro.relay.server.BlobRelayStreamingServer;

class BlobRelayStreamingTest {
    private static final Logger LOG = LoggerFactory.getLogger(BlobRelayStreamingTest.class);
    
    private static final int TEST_DATA_SIZE = 1024 * 10;

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
        server.blockUntilShutdown();
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
        var result = client.put(Streaming.PutStreamingRequest.Metadata.newBuilder()
                                                                            .setSessionId(128)
                                                                      .build(),
                                new ByteArrayInputStream(data));

        // verify received data and response
        assertNotNull(result);
        assertEquals(response.getBlob(), result);
        assertArrayEquals(data, server.receivedData());
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
        client = new BlobRelayStreaming("localhost:" + server.getPort(), false, 1024);
        var inputStream = client.get(Streaming.GetStreamingRequest.newBuilder()
                                                    .setTransactionId(789)
                                                    .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                                                        .setStorageId(1)
                                                        .setObjectId(23)
                                                        .setTag(45))
                                                .build());
        var data = inputStream.readAllBytes();

        // verify received data
        assertArrayEquals(buffer, data);
    }
}