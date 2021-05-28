package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.junit.jupiter.api.Test;

class WireImplTest {
    WireImpl client;
    ServerWireImpl server;
    String wireName = "tsubakuro-session1";

    @Test
    void requestBegin() {
	try {
	    server = new ServerWireImpl(wireName);
	    client = new WireImpl(wireName);

	    var response = client.send(ProtosForTest.BeginRequestChecker.builder().build(), new FutureResponseImpl.BeginDistiller());
	    assertTrue(ProtosForTest.BeginRequestChecker.check(server.get()));

	    server.put(ProtosForTest.BeginResponseChecker.builder().build());
	    assertTrue(ProtosForTest.ResMessageBeginChecker.check(response.get()));

	    client.close();
	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	} catch (InterruptedException e) {
	    fail("cought IOException");
	} catch (ExecutionException e) {
	    fail("cought IOException");
	}
    }
}
