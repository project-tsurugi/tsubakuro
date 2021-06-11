package com.nautilus_technologies.tsubakuro.impl.low.connection;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.ServerWireImpl;

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
	    assertEquals(serverConnection.accept(), 0);

	    var future = IpcConnectorImpl.connect(dbName);
	    var handle = serverConnection.accept();
	    assertEquals(handle, 1);
	    server = new ServerWireImpl(dbName + "-" + String.valueOf(handle));
	    client = (SessionWireImpl) future.get();
	    



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
}
