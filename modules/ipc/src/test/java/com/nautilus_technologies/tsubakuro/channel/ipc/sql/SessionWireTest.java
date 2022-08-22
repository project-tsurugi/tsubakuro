package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.ipc.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;
import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tateyama.proto.SqlResponse;

class SessionWireTest {
    static final int SERVICE_ID_SQL = 3;
    private SessionWireImpl client;
    private ServerWireImpl server;
    private final String dbName = "tsubakuro";
    private final long sessionID = 1;

    @Test
    void requestBegin() throws Exception {
        try {
            server = new ServerWireImpl(dbName, sessionID);
            client = new SessionWireImpl(dbName, sessionID);
        } catch (Exception e) {
            fail("cought Exception");
        }

        CommunicationChecker.check(server, client);

        try {
            client.close();
            server.close();
        } catch (IOException e) {
            fail("cought IOException in close");
        }
    }

    @Test
    void inconsistentResponse() {
        try {
            server = new ServerWireImpl(dbName, sessionID);
            client = new SessionWireImpl(dbName, sessionID);
    
            // REQUEST test begin
            // client side send Request
            var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
            // server side receive Request
            assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionID));
            // REQUEST test end
    
            // RESPONSE test begin
            // server side send Response
            server.put(ProtosForTest.PrepareResponseChecker.builder().build());
    
    
            // client side receive Response, ends up an error
            var response = futureResponse.get();
            var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(response.waitForMainResponse()));
            assertFalse(SqlResponse.Response.ResponseCase.BEGIN.equals(responseReceived.getResponseCase()));
    
            client.close();
            server.close();
        } catch (IOException | ServerException | InterruptedException e) {
            fail("cought IOException in inconsistentResponse");
        }
    }

    @Disabled("time out is not handled")
    @Test
    void timeout() throws Exception {
        server = new ServerWireImpl(dbName, sessionID);
        client = new SessionWireImpl(dbName, sessionID);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
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
