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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.tsubakuro.common.BlobTransferType;
import com.tsurugidb.tsubakuro.common.BlobTransferMedium;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class SessionImplLobTest {
    private static RequestHandler accepts(EndpointRequest.Request.CommandCase command, RequestHandler next) {
        return new RequestHandler() {
            @Override
            public Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException {
                EndpointRequest.Request message = EndpointRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertEquals(command, message.getCommandCase());
                return next.handle(serviceId, request);
            }
        };
    }

    @Test
    void getLargeObjectClient_with_uri_success() throws Exception {
        URI uri = URI.create("dns:///althost:63456");
        MockWire wire = new MockWire();
        wire.setBlobTransferMedium(new BlobTransferMedium() {
            @Override
            public BlobTransferType getBlobTransferType() {
                return BlobTransferType.RELAY;
            }
            @Override
            public Map<String, String> getParameters() {
                return Map.of("sessionId", "246",
                              "endpoint", "dns:///localhost:52345",
                              "stream_chunk_size", "1024");
            }
        });
        wire.next(accepts(EndpointRequest.Request.CommandCase.HANDSHAKE,
                RequestHandler.returns(EndpointResponse.Handshake.newBuilder()
                        .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                            .setSessionId(123)
                            .build())
                            // BlobRelayServiceInfo is not used in this test.
                        .build())));

        try (var session = new SessionImpl(false, null, BlobTransferType.RELAY, uri)) {
            session.connect(wire);

            var largeObjectClient = session.getLargeObjectClient();
            assertTrue(largeObjectClient instanceof LargeObjectClientRelay);
            var largeObjectClientString = largeObjectClient.toString();
            assertTrue(largeObjectClientString.contains("sessionId=246"));
            assertTrue(largeObjectClientString.contains("endpoint='dns:///althost:63456'"));
            assertTrue(largeObjectClientString.contains("chunkSize=1024"));
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void getLargeObjectClient_privileged_fail() throws Exception {
        MockWire wire = new MockWire();
        wire.setBlobTransferMedium(new BlobTransferMedium() {
            @Override
            public BlobTransferType getBlobTransferType() {
                return BlobTransferType.DOES_NOT_USE;
            }
            @Override
            public Map<String, String> getParameters() {
                return Map.of();
            }
        });
        wire.next(accepts(EndpointRequest.Request.CommandCase.HANDSHAKE,
                RequestHandler.returns(EndpointResponse.Handshake.newBuilder()
                        .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                            .setSessionId(123)
                            .build())
                            // BlobRelayServiceInfo is not used in this test.
                        .build())));

        try (var session = new SessionImpl(false, null, BlobTransferType.PRIVILEGED, null)) {
            assertThrows(IllegalStateException.class, () -> session.connect(wire));
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
            fail("caught some exception");
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void getLargeObjectClient_relay_fail() throws Exception {
        MockWire wire = new MockWire();
        wire.setBlobTransferMedium(new BlobTransferMedium() {
            @Override
            public BlobTransferType getBlobTransferType() {
                return BlobTransferType.DOES_NOT_USE;
            }
            @Override
            public Map<String, String> getParameters() {
                return Map.of();
            }
        });
        wire.next(accepts(EndpointRequest.Request.CommandCase.HANDSHAKE,
                RequestHandler.returns(EndpointResponse.Handshake.newBuilder()
                        .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                            .setSessionId(123)
                            .build())
                            // BlobRelayServiceInfo is not used in this test.
                        .build())));

        try (var session = new SessionImpl(false, null, BlobTransferType.RELAY, null)) {
            assertThrows(IllegalStateException.class, () -> session.connect(wire));
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
            fail("caught some exception");
        }
        assertFalse(wire.hasRemaining());
    }
}