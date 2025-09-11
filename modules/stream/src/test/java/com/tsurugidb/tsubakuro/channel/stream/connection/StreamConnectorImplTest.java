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
package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.channel.stream.ServerMock;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

class StreamConnectorImplTest {
    private static final String HOST = "localhost";
    private static final int PORT = 12350;

    private static String encryptionKey() {
        return new String(Base64.getDecoder().decode("LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUFsVjAzbUJISU9LNjBCVm5nVWJvcGUvbVVPRHVSQ2FvZVVqY2hZbEMzMFRhbGFpRklIdjRMRHBqL1pMRDJGdVQwUFNDNE56aWF1c2Q0TGhDaXp5REk2VGUzMTVXZHhxSXl1dkZQV3lPdGtMdTgzcjVuYnJqT0pqaWVYd3BUejdLdk9iYmRqRjVjWFdKRnlzU1UvaGRwUDdOMTRZVXhpVkpuUTZIWk56VTRSNjVhRDdrU1NNL2MzK1h4czFndEpFUzlDSEV3R1kxU0JnUlA4UWx2V1o2QkQzak1WQm0xUVkyY00xS0lrZ1RDZFJNRWRSWWtoTTFSYk9EU0VHZzBXN3dIaXRpUUlVOE83M0I1cElRcE96OXNWS0V4N28ySXk5L2RhbzVTaG5iRTdHWUt2UzlXZXFpbHAxMmF5U1pKeWlQaklLc1VnMWc1N3NBMEVDKzRxZGhHbFFJREFRQUIKLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0t"),
                            StandardCharsets.US_ASCII);
    }

    @Test
    void normal_withoutTimeout() throws Exception {
        var server = new ServerMock(PORT);
        server.next(EndpointResponse.EncryptionKey.newBuilder()
                        .setSuccess(EndpointResponse.EncryptionKey.Success.newBuilder()
                                        .setEncryptionKey(encryptionKey()))
                        .build());
        server.next(EndpointResponse.Handshake.newBuilder()
                        .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                                        .setSessionId(123))
                        .build());

        var clientInformation = new ClientInformation("label", "app", new UsernamePasswordCredential("user", "password"));
        var streamConnectorImpl = new StreamConnectorImpl(HOST, PORT);
        var futureResponse = streamConnectorImpl.connect(clientInformation);
        assertNotNull(futureResponse);
        try {
            var wire = futureResponse.get();
            assertNotNull(wire);
            wire.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            server.close();
        }
        assertFalse(server.hasRemaining());
    }

    @Test
    void normal_withTimeout() throws Exception {
        var server = new ServerMock(PORT + 1);
        server.next(EndpointResponse.EncryptionKey.newBuilder()
                        .setSuccess(EndpointResponse.EncryptionKey.Success.newBuilder()
                                        .setEncryptionKey(encryptionKey()))
                        .build());
        server.next(EndpointResponse.Handshake.newBuilder()
                        .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                                        .setSessionId(123))
                        .build());

        var clientInformation = new ClientInformation("label", "app", new UsernamePasswordCredential("user", "password"));
        var streamConnectorImpl = new StreamConnectorImpl(HOST, PORT + 1);
        var futureResponse = streamConnectorImpl.connect(clientInformation);
        assertNotNull(futureResponse);
        try {
            var wire = futureResponse.get(1, TimeUnit.SECONDS);
            assertNotNull(wire);
            wire.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            server.close();
        }
        assertFalse(server.hasRemaining());
    }

    @Test
    void timeout_encryptionKey() throws Exception {
        var server = new ServerMock(PORT + 2);

        var clientInformation = new ClientInformation("label", "app", new UsernamePasswordCredential("user", "password"));
        var streamConnectorImpl = new StreamConnectorImpl(HOST, PORT + 2);
        var futureResponse = streamConnectorImpl.connect(clientInformation);
        assertNotNull(futureResponse);

        assertThrows(ResponseTimeoutException.class, () -> futureResponse.get(1, TimeUnit.SECONDS));

        server.close();
        assertFalse(server.hasRemaining());
    }

    @Test
    void timeout_handshake() throws Exception {
        var server = new ServerMock(PORT + 3);
        server.next(EndpointResponse.EncryptionKey.newBuilder()
                        .setSuccess(EndpointResponse.EncryptionKey.Success.newBuilder()
                                        .setEncryptionKey(encryptionKey()))
                        .build());

        var clientInformation = new ClientInformation("label", "app", new UsernamePasswordCredential("user", "password"));
        var streamConnectorImpl = new StreamConnectorImpl(HOST, PORT + 3);
        var futureResponse = streamConnectorImpl.connect(clientInformation);
        assertNotNull(futureResponse);

        assertThrows(ResponseTimeoutException.class, () -> futureResponse.get(1, TimeUnit.SECONDS));

        server.close();
        assertFalse(server.hasRemaining());
    }

    @Test
    void noTimeout_encryptionKey() throws Exception {
        var server = new ServerMock(PORT + 4);

        var clientInformation = new ClientInformation("label", "app", new UsernamePasswordCredential("user", "password"));
        var streamConnectorImpl = new StreamConnectorImpl(HOST, PORT + 4);
        var futureResponse = streamConnectorImpl.connect(clientInformation);
        assertNotNull(futureResponse);

        assertThrows(AssertionFailedError.class, () -> {
            assertTimeoutPreemptively(
                Duration.ofSeconds(1), () -> { futureResponse.get(0, TimeUnit.SECONDS); }
            );
        });

        server.close();
        assertFalse(server.hasRemaining());
    }

    @Test
    void noTimeout_handshake() throws Exception {
        var server = new ServerMock(PORT + 5);
        server.next(EndpointResponse.EncryptionKey.newBuilder()
                        .setSuccess(EndpointResponse.EncryptionKey.Success.newBuilder()
                                        .setEncryptionKey(encryptionKey()))
                        .build());

        var clientInformation = new ClientInformation("label", "app", new UsernamePasswordCredential("user", "password"));
        var streamConnectorImpl = new StreamConnectorImpl(HOST, PORT + 5);
        var futureResponse = streamConnectorImpl.connect(clientInformation);
        assertNotNull(futureResponse);

        assertThrows(AssertionFailedError.class, () -> {
            assertTimeoutPreemptively(
                Duration.ofSeconds(1), () -> { futureResponse.get(0, TimeUnit.SECONDS); }
            );
        });

        server.close();
        assertFalse(server.hasRemaining());
    }
}