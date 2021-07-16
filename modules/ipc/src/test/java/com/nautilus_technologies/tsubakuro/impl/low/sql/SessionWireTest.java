package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.ExecuteQueryDistiller;
import com.nautilus_technologies.tsubakuro.protos.BeginDistiller;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

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
    void multipleRequests() {
	try {
	    server = new ServerWireImpl(dbName, sessionID);
	    client = new SessionWireImpl(dbName, sessionID);

	    // client side send multiple requests, which can be stored in thre response_box
	    Queue<Future<ResponseProtos.Begin>> queue = new ArrayDeque<>();
            for (long i = 0; i < 16; i++) {  // 16 is from construction of response_box in test/native/include/server_wires.h
		queue.add(client.send(ProtosForTest.BeginRequestChecker.builder(), new BeginDistiller()));
	    }

	    // client side send one more request
	    Throwable exception = assertThrows(IOException.class, () -> {
		    var response = client.send(ProtosForTest.BeginRequestChecker.builder(), new BeginDistiller());
		});
	    assertEquals("the number of pending requests exceeded the number of response boxes", exception.getMessage());

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
	assertEquals("cannot find a session with the specified name", exception.getMessage());
    }
}
