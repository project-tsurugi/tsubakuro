package com.tsurugidb.tsubakuro.debug.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.debug.proto.DebugRequest;
import com.tsurugidb.debug.proto.DebugRequest.Request;
import com.tsurugidb.debug.proto.DebugResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.debug.DebugServiceCode;
import com.tsurugidb.tsubakuro.debug.DebugServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockWire;
import com.tsurugidb.tsubakuro.mock.RequestHandler;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class DebugServiceStubTest {

    private final MockWire wire = new MockWire();

    private final Session session = new SessionImpl(wire);

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    private static RequestHandler accepts(DebugRequest.Request.CommandCase command, RequestHandler next) {
        return new RequestHandler() {
            @Override
            public Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException {
                Request message = DebugRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertEquals(command, message.getCommandCase());
                return next.handle(serviceId, request);
            }
        };
    }

    private static DebugResponse.Void newVoid() {
        return DebugResponse.Void.newBuilder()
                .build();
    }

    private static DebugResponse.UnknownError newUnknown(String message) {
        return DebugResponse.UnknownError.newBuilder()
                .setMessage(message)
                .build();
    }

    @Test
    void send_Logging_Success() throws Exception {
        wire.next(accepts(DebugRequest.Request.CommandCase.LOGGING,
                RequestHandler.returns(DebugResponse.Logging.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        var message = DebugRequest.Logging.newBuilder()
                .build();
        try (var delegate = new DebugServiceStub(session)) {
            delegate.send(message).await();
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Logging_UnknownError() throws Exception {
        wire.next(RequestHandler.returns(DebugResponse.Logging.newBuilder()
                .setUnknownError(newUnknown("UE"))
                .build()));

        var message = DebugRequest.Logging.newBuilder()
                .build();
        try (var delegate = new DebugServiceStub(session)) {
            var e = assertThrows(DebugServiceException.class, () -> delegate.send(message).await());
            assertEquals(DebugServiceCode.UNKNOWN, e.getDiagnosticCode());
        }
        assertFalse(wire.hasRemaining());
    }
}
