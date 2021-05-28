package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.junit.jupiter.api.Test;

class SessionLinkImplTest {
    WireImpl client;
    ServerWireImpl server;
    String wireName = "tsubakuro-session1";

    @Test
    void requestBeginAndReceiveByFuture() {
	try {
	    server = new ServerWireImpl(wireName);
	    client = new WireImpl(wireName);

	    var sessionLink = new SessionLinkImpl(client);

	    var future = sessionLink.send(ProtosForTest.BeginChecker.builder().build());
	    assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), true));

	    server.put(ProtosForTest.BeginResponseChecker.builder().build());
	    assertTrue(ProtosForTest.ResMessageBeginChecker.check(future.get()));

	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	} catch (InterruptedException e) {
	    fail("cought InterruptedException");
	} catch (ExecutionException e) {
	    fail("cought ExecutionException");
	}
    }
}
