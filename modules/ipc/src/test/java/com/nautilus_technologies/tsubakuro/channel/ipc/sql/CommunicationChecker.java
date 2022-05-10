package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.protos.BeginDistiller;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

public final class CommunicationChecker {
    static final long SERVICE_ID_SQL = 3;
    private CommunicationChecker() {
    }

    public static void check(ServerWireImpl server, SessionWireImpl client) throws IOException, ServerException, InterruptedException {
        // REQUEST test begin
        // client side send Request
        var futureResponse = client.send(SERVICE_ID_SQL, ProtosForTest.BeginRequestChecker.builder(), new BeginDistiller());
        // server side receive Request
        assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), server.getSessionID()));
        // REQUEST test end

        // RESPONSE test begin
        // server side send Response
        server.put(ProtosForTest.BeginResponseChecker.builder().build());
        // client side receive Response
        assertTrue(ProtosForTest.ResMessageBeginChecker.check(futureResponse.get()));
        // RESPONSE test end
    }
}
