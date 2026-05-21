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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.tsubakuro.common.BlobTransferType;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockLink;

class SessionImplWithLinkTest {

    private final MockLink link = new MockLink();

    private final WireImpl wire;

    private final Session session = new SessionImpl();

    public SessionImplWithLinkTest() {
        try {
            wire = new WireImpl(link);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    @Test
    void connect_relay_success() throws Exception {
        link.next(EndpointResponse.Handshake.newBuilder()
            .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                .setSessionId(123)
                .setBlobRelayServiceInfo(EndpointResponse.BlobRelayServiceInfo.newBuilder()
                        .setBlobSessionId(246)
                        .setEndpoint("dns:///localhost:63456")
                        .setMedium("stream")))
            .build());

        var future = wire.handshake(new ClientInformation(), null, 0, null);
        assertNotNull(future);
        long sessionId = future.get();
        assertEquals(123, sessionId);

        try (var session = new SessionImpl()) {
            session.connect(wire);
            var blobTransferMedium = session.getBlobTransferMedium();
            assertEquals(BlobTransferType.RELAY, blobTransferMedium.getBlobTransferType());
            var parameters = blobTransferMedium.getParameters();
            assertNotNull(parameters);
            assertEquals("246", parameters.get("sessionId"));
            assertEquals("dns:///localhost:63456", parameters.get("endpoint"));
            assertEquals("stream", parameters.get("medium"));
            assertTrue(session.getLargeObjectClient() instanceof LargeObjectClientRelay);
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
            fail("cought some exception");
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void connect_privileged_success() throws Exception {
        link.next(EndpointResponse.Handshake.newBuilder()
            .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                .setSessionId(123)
                .setPrivilegedMode(EndpointResponse.Void.newBuilder())
                )
            .build());

        var future = wire.handshake(new ClientInformation(), null, 0, null);
        assertNotNull(future);
        long sessionId = future.get();
        assertEquals(123, sessionId);

        try (var session = new SessionImpl()) {
            session.connect(wire);
            var blobTransferMedium = session.getBlobTransferMedium();
            assertEquals(BlobTransferType.PRIVILEGED, blobTransferMedium.getBlobTransferType());
            assertTrue(session.getLargeObjectClient() instanceof LargeObjectClientPrivileged);
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
            fail("cought some exception");
        }
        assertFalse(link.hasRemaining());
    }
}