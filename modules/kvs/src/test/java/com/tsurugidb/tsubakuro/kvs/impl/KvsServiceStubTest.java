package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsRequest.Request;
import com.tsurugidb.kvs.proto.KvsResponse;
import com.tsurugidb.kvs.proto.KvsTransaction.Handle;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.mock.MockWire;
import com.tsurugidb.tsubakuro.mock.RequestHandler;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class KvsServiceStubTest {

    private final MockWire wire = new MockWire();

    private final Session session = new SessionImpl(wire);

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    private static RequestHandler accepts(KvsRequest.Request.CommandCase command, RequestHandler next) {
        return new RequestHandler() {
            @Override
            public Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException {
                Request req = KvsRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertEquals(command, req.getCommandCase());
                return next.handle(serviceId, request);
            }
        };
    }

    private static KvsResponse.Void newVoid() {
        return KvsResponse.Void.newBuilder()
                .build();
    }

    private static KvsResponse.Error newError(int code, String req) {
        return KvsResponse.Error.newBuilder()
                .setCode(code)
                .setDetail(req)
                .build();
    }

    private static final KvsServiceCode ERR_CODE = KvsServiceCode.INVALID_ARGUMENT;
    private static final String ERR_DETAIL = "error occured";

    private static KvsResponse.Error newError() {
        return newError(ERR_CODE.getCodeNumber(), ERR_DETAIL);
    }

    private static void checkException(KvsServiceException e) {
        assertEquals(ERR_CODE, e.getDiagnosticCode());
        assertTrue(e.getMessage().contains(ERR_CODE.getStructuredCode()));
        assertTrue(e.getMessage().contains(ERR_DETAIL));
    }

    private static KvsResponse.Response newBegin(long systemId) {
        return KvsResponse.Response.newBuilder().setBegin(
                KvsResponse.Begin.newBuilder()
                    .setSuccess(KvsResponse.Begin.Success.newBuilder()
                        .setTransactionHandle(
                                Handle.newBuilder().setSystemId(systemId).build())
                        .build())
                    .build())
                .build();
    }

    private static KvsResponse.Response newRollback() {
        return KvsResponse.Response.newBuilder().setRollback(
                KvsResponse.Rollback.newBuilder()
                .setSuccess(newVoid()).build()).build();
    }

    private static KvsResponse.Response newDispose() {
        return KvsResponse.Response.newBuilder().setDisposeTransaction(
                KvsResponse.DisposeTransaction.newBuilder()
                .setSuccess(newVoid()).build()).build();
    }

    private static RequestHandler newAcceptBegin(long systemId) throws IOException {
        return accepts(KvsRequest.Request.CommandCase.BEGIN,
                RequestHandler.returns(newBegin(systemId)));
    }

    private static RequestHandler newAcceptRollback() throws IOException {
        return accepts(KvsRequest.Request.CommandCase.ROLLBACK,
                RequestHandler.returns(newRollback()));
    }

    private static RequestHandler newAcceptDispose() throws IOException {
        return accepts(KvsRequest.Request.CommandCase.DISPOSE_TRANSACTION,
                RequestHandler.returns(newDispose()));
    }

    @Test
    void send_Begin_Success() throws Exception {
        wire.next(newAcceptBegin(1234L));
        wire.next(newAcceptRollback());
        wire.next(newAcceptDispose());

        var req = KvsRequest.Begin.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            delegate.send(req).await();
            // NOTE: Rollback and DisposeTransaction are automatically called
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Begin_Error() throws Exception {
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setBegin(
                        KvsResponse.Begin.newBuilder()
                            .setError(newError())
                            ).build()));

        var req = KvsRequest.Begin.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Commit_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setCommit(
                    KvsResponse.Commit.newBuilder()
                        .setSuccess(newVoid())
                        .build())
                    .build();
        wire.next(accepts(KvsRequest.Request.CommandCase.COMMIT,
                RequestHandler.returns(res)));

        var req = KvsRequest.Commit.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            delegate.send(req).await();
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Commit_Error() throws Exception {
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setCommit(
                        KvsResponse.Commit.newBuilder()
                            .setError(newError())
                            ).build()));

        var req = KvsRequest.Commit.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Rollback_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setRollback(
                    KvsResponse.Rollback.newBuilder()
                        .setSuccess(newVoid())
                        .build())
                    .build();
        wire.next(accepts(KvsRequest.Request.CommandCase.ROLLBACK,
                RequestHandler.returns(res)));

        var req = KvsRequest.Rollback.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            delegate.send(req).await();
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Rollback_Error() throws Exception {
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setRollback(
                        KvsResponse.Rollback.newBuilder()
                            .setError(newError())
                            ).build()));

        var req = KvsRequest.Rollback.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Put_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setPut(
                    KvsResponse.Put.newBuilder()
                        .setSuccess(
                                KvsResponse.Put.Success.newBuilder().build())
                        .build())
                    .build();
        wire.next(accepts(KvsRequest.Request.CommandCase.PUT,
                RequestHandler.returns(res)));

        var req = KvsRequest.Put.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            delegate.send(req).await();
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Put_Error() throws Exception {
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setPut(
                        KvsResponse.Put.newBuilder()
                            .setError(newError())
                            ).build()));

        var req = KvsRequest.Put.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Get_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setGet(
                    KvsResponse.Get.newBuilder()
                        .setSuccess(
                                KvsResponse.Get.Success.newBuilder().build())
                        .build())
                    .build();
        wire.next(accepts(KvsRequest.Request.CommandCase.GET,
                RequestHandler.returns(res)));

        var req = KvsRequest.Get.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            delegate.send(req).await();
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Get_Error() throws Exception {
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setGet(
                        KvsResponse.Get.newBuilder()
                            .setError(newError())
                            ).build()));

        var req = KvsRequest.Get.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_DisposeTransaction_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setDisposeTransaction(
                    KvsResponse.DisposeTransaction.newBuilder()
                        .setSuccess(newVoid())
                        .build())
                    .build();
        wire.next(accepts(KvsRequest.Request.CommandCase.DISPOSE_TRANSACTION,
                RequestHandler.returns(res)));

        var req = KvsRequest.DisposeTransaction.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            delegate.send(req).await();
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_DisposeTransaction_Error() throws Exception {
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setDisposeTransaction(
                        KvsResponse.DisposeTransaction.newBuilder()
                            .setError(newError())
                            ).build()));

        var req = KvsRequest.DisposeTransaction.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }
}
