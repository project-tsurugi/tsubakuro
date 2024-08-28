/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.net.ConnectException;

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
//        assertEquals(serverConnection.listen(), 0);

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
    void reject() throws Exception {
        ServerConnectionImpl serverConnection;

        serverConnection = new ServerConnectionImpl(dbName);
//        assertEquals(serverConnection.listen(), 0);

        var connector = new IpcConnectorImpl(dbName);
        var future = connector.connect();
        var id = serverConnection.listen();
        assertEquals(id, 1);
        serverConnection.reject();

        Throwable exception = assertThrows(ConnectException.class, () -> {
            future.get();
        });
        assertEquals("IPC connection establishment failure", exception.getMessage());

        serverConnection.close();
    }

    @Test
    void reject_timeout() throws Exception {
        ServerConnectionImpl serverConnection;

        serverConnection = new ServerConnectionImpl(dbName);
//        assertEquals(serverConnection.listen(), 0);

        var connector = new IpcConnectorImpl(dbName);
        var future = connector.connect();
        var id = serverConnection.listen();
        assertEquals(id, 1);
        serverConnection.reject();

        Throwable exception = assertThrows(ConnectException.class, () -> {
            future.get(1, TimeUnit.SECONDS);
        });
        assertEquals("IPC connection establishment failure", exception.getMessage());

        serverConnection.close();
    }

    @Test
    void timeout() throws Exception {
        try (var serverConnection = new ServerConnectionImpl(dbName)) {
//            assertEquals(serverConnection.listen(), 0);

            var connector = new IpcConnectorImpl(dbName);
            var future = connector.connect();
            var id = serverConnection.listen();
            assertEquals(id, 1);

            var start = System.currentTimeMillis();
            Throwable exception = assertThrows(TimeoutException.class, () -> {
                future.get(1, TimeUnit.SECONDS);
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
        //        assertEquals("cannot find a database with the specified name: tsubakuro", exception.getMessage());
        assertTrue(exception.getMessage().contains("cannot find a database with the specified name: tsubakuro"));
    }

    @Test
    void getTwice() throws Exception {
        ServerConnectionImpl serverConnection;
        ServerWireImpl server;

        serverConnection = new ServerConnectionImpl(dbName);

        var connector = new IpcConnectorImpl(dbName);
        var future = connector.connect();
        var id = serverConnection.listen();
        assertEquals(id, 1);
        server = serverConnection.accept(id);

        var client1 = future.get();
        var client2 = future.get();
        assertEquals(client1, client2);

        client1.close();
        serverConnection.close();
        server.close();
    }
}
