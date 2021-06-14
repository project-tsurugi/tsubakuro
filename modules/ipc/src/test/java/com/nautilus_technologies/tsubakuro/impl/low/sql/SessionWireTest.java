package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

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
	    server = new ServerWireImpl(dbName + "-" + String.valueOf(sessionID));
	    client = new SessionWireImpl(dbName, sessionID);

	    CommunicationChecker.check(server, client);

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
