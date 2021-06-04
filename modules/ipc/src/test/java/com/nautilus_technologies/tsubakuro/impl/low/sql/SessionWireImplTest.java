package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.junit.jupiter.api.Test;

class SessionWireImplTest {
    private SessionWireImpl client;
    private ServerWireImpl server;
    private String wireName = "tsubakuro-session1";

    @Test
    void requestBegin() {
	try {
	    server = new ServerWireImpl(wireName);
	    client = new SessionWireImpl(wireName);

	    // REQUEST test begin
	    // client side send Request
	    var futureResponse = client.send(ProtosForTest.BeginRequestChecker.builder().build(), new FutureResponseImpl.BeginDistiller());
	    // server side receive Request
	    assertTrue(ProtosForTest.BeginRequestChecker.check(server.get()));
	    // REQUEST test end

	    // RESPONSE test begin
	    // server side send Response
	    server.put(ProtosForTest.BeginResponseChecker.builder().build());
	    // client side receive Response
	    assertTrue(ProtosForTest.ResMessageBeginChecker.check(futureResponse.get()));
	    // RESPONSE test end

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
