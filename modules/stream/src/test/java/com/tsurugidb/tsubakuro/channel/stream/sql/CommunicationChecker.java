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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.protos.ProtosForTest;

public final class CommunicationChecker {
    static final int SERVICE_ID_SQL = 3;
    private CommunicationChecker() {
    }

    public static void check(ServerWireImpl server, WireImpl client) {
        try {
            // REQUEST test begin
            // client side send Request
            var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
            // server side receive Request
            assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), server.getSessionId()));
            // REQUEST test end

            // RESPONSE test begin
            // server side send Response
            server.put(ProtosForTest.BeginResponseChecker.builder().build());
            // client side receive Response
            ByteBuffer buf = futureResponse.get().waitForMainResponse();
            var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(buf));
            assertTrue(ProtosForTest.ResMessageBeginChecker.check(responseReceived.getBegin()));
            // RESPONSE test end
        } catch (IOException | ServerException | InterruptedException e) {
            fail("cought IOException");
        }
    }
}
