package com.tsurugidb.tsubakuro.channel.ipc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.ipc.sql.CommunicationChecker;
import com.tsurugidb.tsubakuro.channel.ipc.sql.ServerWireImpl;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;

class ConnectionTest {
    private static String dbName = "tsubakuro";

    @Test
    void connect() throws Exception {
        WireImpl client;
        ServerConnectionImpl serverConnection;
        ServerWireImpl server;

        serverConnection = new ServerConnectionImpl(dbName);
        assertEquals(serverConnection.listen(), 0);

        var connector = new IpcConnectorImpl(dbName);
        var future = connector.connect();
        var id = serverConnection.listen();
        assertEquals(id, 1);
        server = serverConnection.accept(id);
        client = (WireImpl) future.get();

        CommunicationChecker.check(server, client);

        client.close();
        serverConnection.close();
        server.close();
    }

    @Test
    void timeout() throws Exception {
        try (var serverConnection = new ServerConnectionImpl(dbName)) {
            assertEquals(serverConnection.listen(), 0);

            var connector = new IpcConnectorImpl(dbName);
            var future = connector.connect();
            var id = serverConnection.listen();
            assertEquals(id, 1);

            var start = System.currentTimeMillis();
            Throwable exception = assertThrows(TimeoutException.class, () -> {
                var client = (WireImpl) future.get(1, TimeUnit.SECONDS);
            });
            assertEquals("connection response has not been accepted within the specified time", exception.getMessage());
            var duration = System.currentTimeMillis() - start;
            assertTrue((750 < duration) && (duration < 1250));
        }
    }

    @Test
    void notExist() {
        var connector = new IpcConnectorImpl(dbName);

        Throwable exception = assertThrows(IOException.class, () -> {
            var future = connector.connect();
        });
        // FIXME: check error code instead of message
        assertEquals("cannot find a database with the specified name: tsubakuro", exception.getMessage());
    }
}
