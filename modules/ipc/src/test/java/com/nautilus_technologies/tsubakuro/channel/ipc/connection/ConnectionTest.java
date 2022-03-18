package com.nautilus_technologies.tsubakuro.channel.ipc.connection;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import com.nautilus_technologies.tsubakuro.channel.ipc.sql.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.channel.ipc.sql.ServerWireImpl;
import com.nautilus_technologies.tsubakuro.channel.ipc.sql.CommunicationChecker;

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
    void timeout() {
	ServerConnectionImpl serverConnection;

	try {
	    serverConnection = new ServerConnectionImpl(dbName);
	    assertEquals(serverConnection.listen(), 0);

	    var connector = new IpcConnectorImpl(dbName);
	    var future = connector.connect();
	    var id = serverConnection.listen();
	    assertEquals(id, 1);

	    var start = System.currentTimeMillis();
	    Throwable exception = assertThrows(TimeoutException.class, () -> {
		    var client = (SessionWireImpl) future.get(1, TimeUnit.SECONDS);
		});
	    assertEquals("connection response has not been accepted within the specified time", exception.getMessage());
	    var duration = System.currentTimeMillis() - start;
	    assertTrue((750 < duration) && (duration < 1250));

	    serverConnection.close();
	} catch (IOException e) {
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
