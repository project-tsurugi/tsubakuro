package com.tsurugidb.tsubakuro.common.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.core.proto.CoreRequest;
import com.tsurugidb.core.proto.CoreResponse;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class SessionImplTest {
    private static final String RS_RD = "relation"; //$NON-NLS-1$

    private final MockWire wire = new MockWire();

    private final Session session = new SessionImpl(wire);

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    private static RequestHandler accepts(CoreRequest.Request.CommandCase command, RequestHandler next) {
        return new RequestHandler() {
            @Override
            public Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException {
                CoreRequest.Request message = CoreRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertEquals(command, message.getCommandCase());
                return next.handle(serviceId, request);
            }
        };
    }

    private static CoreResponse.Void newVoid() {
        return CoreResponse.Void.newBuilder()
                .build();
    }

    private static CoreResponse.UnknownError newEngineError() {
        return CoreResponse.UnknownError.newBuilder()
                .setMessage("UNKNOWN_ERROR_FOR_TEST")
                .build();
    }

    @Test
    void updateExpirationTime_success() throws Exception {
        wire.next(accepts(CoreRequest.Request.CommandCase.UPDATE_EXPIRATION_TIME,
                RequestHandler.returns(CoreResponse.UpdateExpirationTime.newBuilder()
                        .setSuccess(newVoid())
                        .build())));

        try (var session = new SessionImpl()) {
            session.connect(wire);
            session.updateExpirationTime(300, TimeUnit.MINUTES).get();
        } catch (Exception e) {
            e.printStackTrace();
            fail("cought some exception");
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void updateExpirationTime_fail() throws Exception {
        wire.next(accepts(CoreRequest.Request.CommandCase.UPDATE_EXPIRATION_TIME,
                RequestHandler.returns(CoreResponse.UpdateExpirationTime.newBuilder()
                        .setUnknownError(newEngineError())
                        .build())));

        try (var session = new SessionImpl()) {
            session.connect(wire);
            Throwable exception = assertThrows(ServerException.class, () -> {
                session.updateExpirationTime(300, TimeUnit.MINUTES).get();;
            });
        }
        assertFalse(wire.hasRemaining());
    }
}
