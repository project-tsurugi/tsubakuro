package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.protos.BeginDistiller;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

public final class CommunicationChecker {
    private CommunicationChecker() {
    }

    public static void check(ServerWireImpl server, SessionWireImpl client) {
	try {

	    // REQUEST test begin
	    // client side send Request
	    var futureResponse = client.send(ProtosForTest.BeginRequestChecker.builder(), new BeginDistiller());
	    // server side receive Request
	    assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), server.getSessionID()));
	    // REQUEST test end

	    // RESPONSE test begin
	    // server side send Response
	    server.put(ProtosForTest.BeginResponseChecker.builder().build());
	    // client side receive Response
	    assertTrue(ProtosForTest.ResMessageBeginChecker.check(futureResponse.get()));
	    // RESPONSE test end

	} catch (IOException e) {
	    fail("cought IOException");
	} catch (InterruptedException e) {
	    fail("cought IOException");
	} catch (ExecutionException e) {
	    fail("cought IOException");
	}
    }
}
