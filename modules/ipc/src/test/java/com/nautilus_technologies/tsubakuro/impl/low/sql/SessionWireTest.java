package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ExecutionException;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.junit.jupiter.api.Test;

class SessionWireTest {
    private SessionWireImpl client;
    private ServerWireImpl server;
    private String dbName = "tsubakuro";
    private long sessionID = 1;

    @Test
    void requestBegin() {
	try {
	    server = new ServerWireImpl(dbName, sessionID);
	    client = new SessionWireImpl(dbName, sessionID);

	    CommunicationChecker.check(server, client);

	    client.close();
	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	}
    }

    @Test
    void inconsistentResponse() {
	try {
	    server = new ServerWireImpl(dbName, sessionID);
	    client = new SessionWireImpl(dbName, sessionID);

	    // REQUEST test begin
	    // client side send Request
	    var futureResponse = client.send(ProtosForTest.BeginRequestChecker.builder(), new BeginDistiller());
	    // server side receive Request
	    assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionID));
	    // REQUEST test end

	    // RESPONSE test begin
	    // server side send Response
	    server.put(ProtosForTest.PrepareResponseChecker.builder().build());

	    // client side receive Response, ends up an error
	    Throwable exception = assertThrows(ExecutionException.class, () -> {
		    var message = futureResponse.get();
		});
	    assertEquals("java.io.IOException: response type is inconsistent with the request type", exception.getMessage());
	    
	    client.close();
	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	}
    }

    @Test
    void notExist() {
        Throwable exception = assertThrows(IOException.class, () -> {
		client = new SessionWireImpl(dbName, sessionID); // not exist
	    });
	assertEquals("cannot find a session wire with the specified name", exception.getMessage());
    }
}
