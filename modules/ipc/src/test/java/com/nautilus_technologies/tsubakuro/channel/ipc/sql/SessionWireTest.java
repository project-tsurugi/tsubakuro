package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.ipc.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.protos.BeginDistiller;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

class SessionWireTest {
    static final long SERVICE_ID_SQL = 3;
    private SessionWireImpl client;
    private ServerWireImpl server;
    private final String dbName = "tsubakuro";
    private final long sessionID = 1;

    @Test
    void requestBegin() throws Exception {
        server = new ServerWireImpl(dbName, sessionID);
        client = new SessionWireImpl(dbName, sessionID);

        CommunicationChecker.check(server, client);

        client.close();
        server.close();
    }

    @Test
    void inconsistentResponse() throws Exception {
        server = new ServerWireImpl(dbName, sessionID);
        client = new SessionWireImpl(dbName, sessionID);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.send(SERVICE_ID_SQL, ProtosForTest.BeginRequestChecker.builder(), new BeginDistiller());
        // server side receive Request
        assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionID));
        // REQUEST test end

        // RESPONSE test begin
        // server side send Response
        server.put(ProtosForTest.PrepareResponseChecker.builder().build());

        // client side receive Response, ends up an error
        Throwable exception = assertThrows(IOException.class, () -> {
            var message = futureResponse.get();
        });
        // FIXME: check error code instead of message
        assertEquals("response type is inconsistent with the request type", exception.getMessage());

        client.close();
        server.close();
    }

    @Test
    void timeout() throws Exception {
        server = new ServerWireImpl(dbName, sessionID);
        client = new SessionWireImpl(dbName, sessionID);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.send(SERVICE_ID_SQL, ProtosForTest.BeginRequestChecker.builder(), new BeginDistiller());
        // server side receive Request
        assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionID));
        // REQUEST test end

        // RESPONSE test begin
        // server side does not send Response

        var start = System.currentTimeMillis();
        // client side receive Response, ends up with timeout error
        Throwable exception = assertThrows(TimeoutException.class, () -> {
            var message = futureResponse.get(1, TimeUnit.SECONDS);
        });
        // FIXME: check error code instead of message
        assertEquals("response has not been received within the specified time", exception.getMessage());
        var duration = System.currentTimeMillis() - start;
        assertTrue((750 < duration) && (duration < 1250));

        client.close();
        server.close();
    }

    @Test
    void notExist() {
        Throwable exception = assertThrows(IOException.class, () -> {
            client = new SessionWireImpl(dbName, sessionID); // not exist
        });
        // FIXME: check error code instead of message
        assertEquals("cannot find a session with the specified name", exception.getMessage());
    }
}
