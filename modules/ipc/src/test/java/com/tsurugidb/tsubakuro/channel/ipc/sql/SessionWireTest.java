package com.tsurugidb.tsubakuro.channel.ipc.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.ipc.IpcLink;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.protos.ProtosForTest;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.sql.proto.SqlResponse;

class SessionWireTest {
    static final int SERVICE_ID_SQL = 3;
    private WireImpl client;
    private ServerWireImpl server;
    private final String dbName = "tsubakuro";
    private final long sessionId = 1;
//    private final String linkLostMessage = "IPC connection failure";

    @Test
    void requestBegin() throws Exception {
        try {
            server = new ServerWireImpl(dbName, sessionId);
            client = new WireImpl(new IpcLink(dbName, sessionId));
            client.handshake(new ClientInformation(), null);
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
            server = new ServerWireImpl(dbName, sessionId);
            client = new WireImpl(new IpcLink(dbName, sessionId));
            client.handshake(new ClientInformation(), null);

            // REQUEST test begin
            // client side send Request
            var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
            // server side receive Request
            assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionId));
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

    @Test
    void timeout() throws Exception {
        server = new ServerWireImpl(dbName, sessionId);
        client = new WireImpl(new IpcLink(dbName, sessionId));
        client.handshake(new ClientInformation(), null);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
        // server side receive Request
        assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionId));
        // REQUEST test end

        // RESPONSE test begin
        // server side does not send Response

        var start = System.currentTimeMillis();
        // client side receive Response, ends up with timeout error
        Throwable exception = assertThrows(TimeoutException.class, () -> {
            var response = futureResponse.get();
            var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(response.waitForMainResponse(1, TimeUnit.SECONDS)));
        });
        // FIXME: check error code instead of message
        assertEquals("response has not been received within the specified time", exception.getMessage());
        var duration = System.currentTimeMillis() - start;
        assertTrue((750 < duration) && (duration < 1250));
        client.close();
        server.close();
    }

    @Test
    void serverCrashDetectionTestWithoutTimeout() throws Exception {
        server = new ServerWireImpl(dbName, sessionId, false);
        client = new WireImpl(new IpcLink(dbName, sessionId));
        client.handshake(new ClientInformation(), null);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
        // server side receive Request
        assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionId));
        // REQUEST test end

        // RESPONSE test begin
        // server side does not send Response

        var start = System.currentTimeMillis();
        // client side receive Response, ends up with server crashed error
        Throwable exception = assertThrows(IOException.class, () -> {
            var response = futureResponse.get();
            var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(response.waitForMainResponse()));
        });
        // FIXME: check error code instead of message
        // assertEquals(linkLostMessage, exception.getMessage());
        var duration = System.currentTimeMillis() - start;
        assertTrue((4000 < duration) && (duration < 11000));
        client.close();
        server.close();
    }

    @Test
    void serverCrashDetectionTestWithTimeout() throws Exception {
        server = new ServerWireImpl(dbName, sessionId);
        client = new WireImpl(new IpcLink(dbName, sessionId));
        client.handshake(new ClientInformation(), null);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.send(SERVICE_ID_SQL, DelimitedConverter.toByteArray(ProtosForTest.BeginRequestChecker.builder().build()));
        // server side receive Request
        assertTrue(ProtosForTest.BeginRequestChecker.check(server.get(), sessionId));
        // REQUEST test end

        // RESPONSE test begin
        // server side does not send Response

        var start = System.currentTimeMillis();
        // client side receive Response, ends up with server crashed error
        Throwable exception = assertThrows(IOException.class, () -> {
            var response = futureResponse.get();
            var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(response.waitForMainResponse(60, TimeUnit.SECONDS)));
        });
        // FIXME: check error code instead of message
        // assertEquals(linkLostMessage, exception.getMessage());
        var duration = System.currentTimeMillis() - start;
        assertTrue((4000 < duration) && (duration < 11000));
        client.close();
        server.close();
    }

    @Test
    void notExist() {
        Throwable exception = assertThrows(IOException.class, () -> {
            client = new WireImpl(new IpcLink(dbName, sessionId)); // not exist
        });
        // FIXME: check error code instead of message
        assertEquals("cannot find a session with the specified name", exception.getMessage());
    }
}
