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
package com.tsurugidb.tsubakuro.channel.stream.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.protos.ProtosForTest;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.sql.proto.SqlResponse;

class SessionWireTest {
    static final int SERVICE_ID_SQL = 3;
    private static final String HOST = "localhost";
    private static final int PORT = 12344;

    private final long sessionId = 1;

    @Test
    void requestBegin() {
        try (
            ServerWireImpl server = new ServerWireImpl(PORT, sessionId);
            var link = new StreamLink(HOST, PORT);
            WireImpl client = new WireImpl(link);
        ) {
            link.setSessionId(sessionId);
            CommunicationChecker.check(server, client);
        } catch (Exception e) {
            e.printStackTrace();
            fail("cought Exception");
        }

    }

    @Test
    void inconsistentResponse() {
        try (
            ServerWireImpl server = new ServerWireImpl(PORT - 1, sessionId);
            var link = new StreamLink(HOST, PORT - 1);
            WireImpl client = new WireImpl(link);
        ) {
            link.setSessionId(sessionId);

            // REQUEST test begin
            // client side send Request
            var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
            // server side receive Request
            assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionId));
            // REQUEST test end

            // RESPONSE test begin
            // server side send Response
            server.put(ProtosForTest.PrepareResponseChecker.builder().build());

            // client side receive Response, ends up an error
            var response = futureResponse.get();
            var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(response.waitForMainResponse()));
            assertFalse(SqlResponse.Response.ResponseCase.BEGIN.equals(responseReceived.getResponseCase()));

        } catch (IOException | ServerException | InterruptedException e) {
            e.printStackTrace();
            fail("cought IOException");
        }
    }

    @Test
    void timeout() {
        try (
            ServerWireImpl server = new ServerWireImpl(PORT - 2, sessionId);
            var link = new StreamLink(HOST, PORT - 2);
            WireImpl client = new WireImpl(link);
        ) {
            link.setSessionId(sessionId);

            // REQUEST test begin
            // client side send Request
            var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
            // server side receive Request
            assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionId));
            // REQUEST test end

            // RESPONSE test begin
            // server side does not send Response

            var start = System.currentTimeMillis();
            // client side receive Response, ends up with timeout error
            Throwable exception = assertThrows(TimeoutException.class, () -> {
                    var response = futureResponse.get();
                    var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(response.waitForMainResponse(1, TimeUnit.SECONDS)));
            });
            assertEquals("response has not been received within the specified time", exception.getMessage());
            var duration = System.currentTimeMillis() - start;
            assertTrue((750 < duration) && (duration < 1250));

        } catch (IOException | InterruptedException | ServerException e) {
            e.printStackTrace();
            fail("cought IOException");
        }
    }

    @Test
    void notExist() {
        Throwable exception = assertThrows(IOException.class, () -> {
            var link = new StreamLink(HOST, PORT - 3);
            link.setSessionId(sessionId);
            WireImpl client = new WireImpl(link);
        });
        assertTrue(exception.getMessage().contains("Connection refused"));
    }
}
