package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockWire;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

class TransactionHandlImplTest {

    @Test
    void basic() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, null)) {
            assertEquals(systemId, tx.getSystemId());
            assertNotEquals(null, tx.getHandle());
        }
    }

    @Test
    void setAutoDispose() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, null)) {
            assertEquals(false, tx.setCommitAutoDisposed());
            assertEquals(true, tx.setCommitAutoDisposed());
        }
    }

    @Test
    void commitCalled() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, null)) {
            assertEquals(false, tx.setCommitCalled());
            assertEquals(true, tx.setCommitCalled());
            assertEquals(true, tx.clearCommitCalled());
            assertEquals(false, tx.clearCommitCalled());
        }
    }

    @Test
    void rollbackCalled() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, null)) {
            assertEquals(false, tx.setRollbackCalled());
            assertEquals(true, tx.setRollbackCalled());
            assertEquals(true, tx.clearCommitCalled());
            assertEquals(false, tx.clearCommitCalled());
        }
    }

    @Test
    void singleClose() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, null)) {
            tx.close();
        }
    }

    @Test
    void doubleClose() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, null)) {
            tx.close();
            tx.close();
        }
    }

    @Test
    void holderClose() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, new ServerResourceHolder())) {
            assertEquals(systemId, tx.getSystemId());
        }
    }

    @Test
    void holderSingleClose() throws Exception {
        final long systemId = 1234L;
        try (var tx = new TransactionHandleImpl(systemId, null, new ServerResourceHolder())) {
            tx.close();
        }
    }

    private final MockWire wire = new MockWire();
    private final Session session = new SessionImpl(wire);

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    @Test
    void commitAutoClose() throws Exception {
        wire.next(StubUtils.newAcceptCommit());
        wire.next(StubUtils.newAcceptDispose());

        final long systemId = 1234L;
        try (var service = new KvsServiceStub(session)) {
            try (var tx = new TransactionHandleImpl(systemId, service, new ServerResourceHolder())) {
                assertEquals(2, wire.size());
                service.send(KvsRequest.Commit.newBuilder().setTransactionHandle(tx.getHandle()).build()).await();
                assertEquals(false, tx.setCommitCalled());
                assertEquals(1, wire.size());
                // NOTE: DisposeTransaction will called at tx.close()
            }
            assertEquals(0, wire.size());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void commitAutoDispose() throws Exception {
        wire.next(StubUtils.newAcceptCommit());

        final long systemId = 1234L;
        try (var service = new KvsServiceStub(session)) {
            try (var tx = new TransactionHandleImpl(systemId, service, new ServerResourceHolder())) {
                assertEquals(1, wire.size());
                assertEquals(false, tx.setCommitAutoDisposed());
                // tx is disposed during commit operation at server side
                service.send(KvsRequest.Commit.newBuilder().setTransactionHandle(tx.getHandle()).build()).await();
                assertEquals(false, tx.setCommitCalled());
                // NOTE: DisposeTransaction will NOT called at tx.close()
                assertEquals(0, wire.size());
            }
            assertEquals(0, wire.size());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void rollbackAutoClose() throws Exception {
        wire.next(StubUtils.newAcceptRollback());
        wire.next(StubUtils.newAcceptDispose());

        final long systemId = 1234L;
        try (var service = new KvsServiceStub(session)) {
            try (var tx = new TransactionHandleImpl(systemId, service, new ServerResourceHolder())) {
                service.send(KvsRequest.Rollback.newBuilder().setTransactionHandle(tx.getHandle()).build()).await();
                assertEquals(false, tx.setRollbackCalled());
                assertEquals(1, wire.size());
            }
            assertEquals(0, wire.size());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void autoRollbackClose() throws Exception {
        wire.next(StubUtils.newAcceptRollback());
        wire.next(StubUtils.newAcceptDispose());

        final long systemId = 1234L;
        try (var service = new KvsServiceStub(session)) {
            try (var tx = new TransactionHandleImpl(systemId, service, new ServerResourceHolder())) {
                // NOTE: rollback and dispose will be called at tx.close()
                assertEquals(2, wire.size());
            }
            assertEquals(0, wire.size());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void manualClose() throws Exception {
        wire.next(StubUtils.newAcceptRollback());
        wire.next(StubUtils.newAcceptDispose());

        final long systemId = 1234L;
        try (var service = new KvsServiceStub(session)) {
            try (var tx = new TransactionHandleImpl(systemId, service, new ServerResourceHolder())) {
                assertEquals(2, wire.size());
                tx.close();
                assertEquals(0, wire.size());
            }
        }
        assertFalse(wire.hasRemaining());
    }

}
