package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsResponse;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.mock.MockWire;
import com.tsurugidb.tsubakuro.mock.RequestHandler;

class KvsServiceStubTest {

    private final MockWire wire = new MockWire();
    private final Session session = new SessionImpl(wire);

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    @Test
    void send_Begin_Success() throws Exception {
        wire.next(StubUtils.newAcceptBegin(1234L));
        wire.next(StubUtils.newAcceptRollback());
        wire.next(StubUtils.newAcceptDispose());

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
                            .setError(StubUtils.newError())
                            ).build()));

        var req = KvsRequest.Begin.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            StubUtils.checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Commit_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setCommit(
                    KvsResponse.Commit.newBuilder()
                        .setSuccess(StubUtils.newVoid())
                        .build())
                    .build();
        wire.next(StubUtils.accepts(KvsRequest.Request.CommandCase.COMMIT,
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
                            .setError(StubUtils.newError())
                            ).build()));

        var req = KvsRequest.Commit.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            StubUtils.checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_Rollback_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setRollback(
                    KvsResponse.Rollback.newBuilder()
                        .setSuccess(StubUtils.newVoid())
                        .build())
                    .build();
        wire.next(StubUtils.accepts(KvsRequest.Request.CommandCase.ROLLBACK,
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
                            .setError(StubUtils.newError())
                            ).build()));

        var req = KvsRequest.Rollback.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            StubUtils.checkException(e);
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
        wire.next(StubUtils.accepts(KvsRequest.Request.CommandCase.PUT,
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
                            .setError(StubUtils.newError())
                            ).build()));

        var req = KvsRequest.Put.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            StubUtils.checkException(e);
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
        wire.next(StubUtils.accepts(KvsRequest.Request.CommandCase.GET,
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
                            .setError(StubUtils.newError())
                            ).build()));

        var req = KvsRequest.Get.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            StubUtils.checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void send_DisposeTransaction_Success() throws Exception {
        var res = KvsResponse.Response.newBuilder().setDisposeTransaction(
                    KvsResponse.DisposeTransaction.newBuilder()
                        .setSuccess(StubUtils.newVoid())
                        .build())
                    .build();
        wire.next(StubUtils.accepts(KvsRequest.Request.CommandCase.DISPOSE_TRANSACTION,
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
                            .setError(StubUtils.newError())
                            ).build()));

        var req = KvsRequest.DisposeTransaction.newBuilder().build();
        try (var delegate = new KvsServiceStub(session)) {
            var e = assertThrows(KvsServiceException.class, () -> delegate.send(req).await());
            StubUtils.checkException(e);
        }
        assertFalse(wire.hasRemaining());
    }
}
