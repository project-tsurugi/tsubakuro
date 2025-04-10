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
package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import javax.annotation.Nonnull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsResponse;
import com.tsurugidb.kvs.proto.KvsTransaction;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.PutResult;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.RemoveResult;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.mock.MockWire;
import com.tsurugidb.tsubakuro.mock.RequestHandler;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class KvsClientImplTest {

    @Test
    void beginTransaction() throws Exception {
        long systemId = 123;
        KvsClient client = new KvsClientImpl(new KvsService() {
            @Override
            public FutureResponse<TransactionHandle> send(KvsRequest.Begin request) throws IOException {
                return FutureResponse.returns(new TransactionHandleImpl(systemId));
            }
        });
        try (var handle = client.beginTransaction().await()) {
            assertEquals(systemId, ((TransactionHandleImpl)handle).getSystemId());
        }
    }

    static class KvsServiceWithExtract implements KvsService {
        @Override
        public KvsTransaction.Handle extract(@Nonnull TransactionHandle handle) {
            if (handle instanceof TransactionHandleImpl) {
                var imp = (TransactionHandleImpl) handle;
                var builder = KvsTransaction.Handle.newBuilder().setSystemId(imp.getSystemId());
                return builder.build();
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Test
    void commit() throws Exception {
        KvsClient client = new KvsClientImpl(new KvsServiceWithExtract() {
            @Override
            public FutureResponse<Void> send(KvsRequest.Commit request) throws IOException {
                assertEquals(KvsRequest.CommitStatus.COMMIT_STATUS_UNSPECIFIED, request.getNotificationType());
                // NOTE assume default value of AutoDispose is true
                assertEquals(true, request.getAutoDispose());
                assertEquals(123, request.getTransactionHandle().getSystemId());
                return FutureResponse.returns(null);
            }
        });
        TransactionHandle handle = new TransactionHandleImpl(123);
        client.commit(handle).await();
    }

    @Test
    void put() throws Exception {
        KvsClient client = new KvsClientImpl(new KvsServiceWithExtract() {
            @Override
            public FutureResponse<PutResult> send(KvsRequest.Put request) throws IOException {
                assertEquals(KvsRequest.Put.Type.OVERWRITE, request.getType());
                return FutureResponse.returns(new PutResultImpl(1));
            }
        });
        TransactionHandle handle = new TransactionHandleImpl(123);
        var buffer = new RecordBuffer();
        buffer.add("key", 100);
        buffer.add("foo", "Hello");
        buffer.add("bar", new BigDecimal("3.14"));
        var result = client.put(handle, "TABLE", buffer).await();
        assertEquals(1, result.size());
    }

    @Test
    void get() throws Exception {
        var buffer = new RecordBuffer();
        buffer.add("key", 100);
        buffer.add("foo", "Hello");
        buffer.add("bar", new BigDecimal("3.14"));
        KvsClient client = new KvsClientImpl(new KvsServiceWithExtract() {
            @Override
            public FutureResponse<GetResult> send(KvsRequest.Get request) throws IOException {
                var records = new ArrayList<KvsData.Record>(1);
                records.add(buffer.toRecord().getEntity());
                return FutureResponse.returns(new GetResultImpl(records));
            }
        });
        TransactionHandle handle = new TransactionHandleImpl(123);
        var result = client.get(handle, "TABLE", buffer).await();
        assertEquals(1, result.size());
        var org = buffer.toRecord();
        var record = result.asRecord();
        assertEquals(org.size(), record.size());
        for (int i = 0; i < buffer.size(); i++) {
            assertEquals(org.getValue(i), record.getValue(i));
        }
        for (int i = 0; i < buffer.size(); i++) {
            assertEquals(org.getName(i), record.getName(i));
        }
        for (int i = 0; i < buffer.size(); i++) {
            assertEquals(org.getValue(i), record.getValue(org.getName(i)));
        }
    }

    @Test
    void remove() throws Exception {
        var buffer = new RecordBuffer();
        buffer.add("key", 100);
        buffer.add("foo", "Hello");
        buffer.add("bar", new BigDecimal("3.14"));
        KvsClient client = new KvsClientImpl(new KvsServiceWithExtract() {
            @Override
            public FutureResponse<RemoveResult> send(KvsRequest.Remove request) throws IOException {
                assertEquals(KvsRequest.Remove.Type.COUNTING, request.getType());
                return FutureResponse.returns(new RemoveResultImpl(1));
            }
        });
        TransactionHandle handle = new TransactionHandleImpl(123);
        var result = client.remove(handle, "TABLE", buffer).await();
        assertEquals(1, result.size());
    }

    //////////////////////////////////////////////////////////////////////////////////

    private final MockWire wire = new MockWire();
    private final Session session = new SessionImpl();

    public KvsClientImplTest() {
        session.connect(wire);
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        session.close();
    }

    @Test
    void autoRollbackClose() throws Exception {
        final long systemId = 1234L;
        wire.next(StubUtils.newAcceptBegin(systemId));
        wire.next(StubUtils.newAcceptRollback());
        wire.next(StubUtils.newAcceptDispose());

        KvsClient client = new KvsClientImpl(new KvsServiceStub(session));
        try (var tx = client.beginTransaction().await()) {
            assertEquals(TransactionHandleImpl.class, tx.getClass());
            // neither commit nor rollback called
            // NOTE: rollback and dispose will be called at tx.close()
            assertEquals(2, wire.size());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void commitAutoDispose() throws Exception {
        final long systemId = 1234L;
        wire.next(StubUtils.newAcceptBegin(systemId));
        wire.next(StubUtils.newAcceptCommit());

        KvsClient client = new KvsClientImpl(new KvsServiceStub(session));
        try (var tx = client.beginTransaction().await()) {
            assertEquals(1, wire.size());
            client.commit(tx).await();
            assertEquals(0, wire.size());
            // NOTE: tx is disposed at server side
            // disposeTransaction will NOT be called
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void commitFailedAutoClose() throws Exception {
        final long systemId = 1234L;
        wire.next(StubUtils.newAcceptBegin(systemId));
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setCommit(
                        KvsResponse.Commit.newBuilder()
                            .setError(StubUtils.newError())
                            ).build()));
        wire.next(StubUtils.newAcceptDispose());

        KvsClient client = new KvsClientImpl(new KvsServiceStub(session));
        try (var tx = client.beginTransaction().await()) {
            assertEquals(2, wire.size());
            var e = assertThrows(KvsServiceException.class, () -> client.commit(tx).await());
            StubUtils.checkException(e);
            assertEquals(1, wire.size());
            // NOTE: tx is NOT disposed at server side
            // disposeTransaction will be called
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void rollbackAutoClose() throws Exception {
        final long systemId = 1234L;
        wire.next(StubUtils.newAcceptBegin(systemId));
        wire.next(StubUtils.newAcceptRollback());
        wire.next(StubUtils.newAcceptDispose());

        KvsClient client = new KvsClientImpl(new KvsServiceStub(session));
        try (var tx = client.beginTransaction().await()) {
            assertEquals(2, wire.size());
            client.rollback(tx).await();
            // NOTE: tx is NOT disposed at server side
            // dispose will be called at tx.close()
            assertEquals(1, wire.size());
        }
        assertFalse(wire.hasRemaining());
    }

    @Test
    void rollbackFailedAutoClose() throws Exception {
        final long systemId = 1234L;
        wire.next(StubUtils.newAcceptBegin(systemId));
        wire.next(RequestHandler.returns(
                KvsResponse.Response.newBuilder().setRollback(
                        KvsResponse.Rollback.newBuilder()
                            .setError(StubUtils.newError())
                            ).build()));
        wire.next(StubUtils.newAcceptDispose());

        KvsClient client = new KvsClientImpl(new KvsServiceStub(session));
        try (var tx = client.beginTransaction().await()) {
            assertEquals(2, wire.size());
            var e = assertThrows(KvsServiceException.class, () -> client.rollback(tx).await());
            StubUtils.checkException(e);
            assertEquals(1, wire.size());
            // NOTE: tx is NOT disposed at server side
            // dispose will be called at tx.close()
        }
        assertFalse(wire.hasRemaining());
    }
}

