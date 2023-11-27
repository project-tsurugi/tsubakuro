package com.tsurugidb.tsubakuro.channel.stream.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.protos.ProtosForTest;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.sql.proto.SqlResponse;

class SessionWireTest {
    static final int SERVICE_ID_SQL = 3;
    private static final String HOST = "localhost";
    private static final int PORT = 12344;

    private final String dbName = "tsubakuro";
    private final long sessionID = 1;

    @Test
    void requestBegin() {
        try (
            ServerWireImpl server = new ServerWireImpl(PORT, sessionID);
            WireImpl client = new WireImpl(new StreamLink(HOST, PORT), sessionID);
        ) {
            CommunicationChecker.check(server, client);
        } catch (Exception e) {
            e.printStackTrace();
            fail("cought Exception");
        }

    }

    @Test
    void inconsistentResponse() {
        try (
            ServerWireImpl server = new ServerWireImpl(PORT - 1, sessionID);
            WireImpl client = new WireImpl(new StreamLink(HOST, PORT - 1), sessionID);
        ) {

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

        } catch (IOException | ServerException | InterruptedException e) {
            e.printStackTrace();
            fail("cought IOException");
        }
    }

    @Test
    void timeout() {
        try (
            ServerWireImpl server = new ServerWireImpl(PORT - 2, sessionID);
            WireImpl client = new WireImpl(new StreamLink(HOST, PORT - 2), sessionID);
        ) {

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
                    var response = futureResponse.get();
                    var responseReceived = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(response.waitForMainResponse(1, TimeUnit.SECONDS)));
            });
            assertEquals("response has not been received within the specified time", exception.getMessage());
            var duration = System.currentTimeMillis() - start;
            assertTrue((750 < duration) && (duration < 1250));

        } catch (IOException e) {
            e.printStackTrace();
            fail("cought IOException");
        }
    }

    @Test
    void notExist() {
        Throwable exception = assertThrows(IOException.class, () -> {
            WireImpl client = new WireImpl(new StreamLink(HOST, PORT - 3), sessionID);
        });
        assertTrue(exception.getMessage().contains("Connection refused"));
    }
}
