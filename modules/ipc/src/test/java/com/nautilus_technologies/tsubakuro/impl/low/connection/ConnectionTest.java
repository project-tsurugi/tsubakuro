package com.nautilus_technologies.tsubakuro.impl.low.connection;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.ServerWireImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.CommunicationChecker;

import org.junit.jupiter.api.Test;

class ConnectionTest {
    private static String dbName = "tsubakuro";

    @Test
    void connect() {
	SessionWireImpl client;
	ServerConnectionImpl serverConnection;
	ServerWireImpl server;

	try {
	    serverConnection = new ServerConnectionImpl(dbName);
	    assertEquals(serverConnection.listen(), 0);

	    var connector = new IpcConnectorImpl(dbName);
	    var future = connector.connect();
	    var id = serverConnection.listen();
	    assertEquals(id, 1);
	    server = serverConnection.accept(id);
	    client = (SessionWireImpl) future.get();

	    CommunicationChecker.check(server, client);

	    client.close();
	    serverConnection.close();
	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	} catch (InterruptedException e) {
	    fail("cought IOException");
	} catch (ExecutionException e) {
	    fail("cought IOException");
	}
    }

    @Test
    void notExist() {
	var connector = new IpcConnectorImpl(dbName);

        Throwable exception = assertThrows(IOException.class, () -> {
		var future = connector.connect();
	    });
	assertEquals("cannot find a database with the specified name: tsubakuro", exception.getMessage());
    }
}
