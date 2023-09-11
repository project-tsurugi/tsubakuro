package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import javax.annotation.Nonnull;

import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsTransaction;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutResult;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.RemoveResult;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
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
                assertEquals(KvsRequest.Commit.Type.COMMIT_TYPE_UNSPECIFIED, request.getType());
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
}

