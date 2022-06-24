package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nautilus_technologies.tsubakuro.channel.stream.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

public final class CommunicationChecker {
    static final int SERVICE_ID_SQL = 3;
    private CommunicationChecker() {
    }

    public static void check(ServerWireImpl server, SessionWireImpl client) {
        try {
            // REQUEST test begin
            // client side send Request
            var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
            // server side receive Request
            assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), server.getSessionID()));
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
