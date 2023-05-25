package com.tsurugidb.tsubakuro.kvs.bench;

import java.io.IOException;
import java.util.LinkedList;

import javax.annotation.Nonnull;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsTransaction;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.PutResult;
import com.tsurugidb.tsubakuro.kvs.RemoveResult;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.kvs.impl.GetResultImpl;
import com.tsurugidb.tsubakuro.kvs.impl.PutResultImpl;
import com.tsurugidb.tsubakuro.kvs.impl.RemoveResultImpl;
import com.tsurugidb.tsubakuro.kvs.impl.KvsService;
import com.tsurugidb.tsubakuro.kvs.impl.TransactionHandleImpl;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class KvsServiceStubForBench implements KvsService {

    private long systemId = System.currentTimeMillis();

    private final RecordBuilder recBuilder;

    KvsServiceStubForBench(RecordInfo info) {
        this.recBuilder = new RecordBuilder(info);
    }

    @Override
    public KvsTransaction.Handle extract(@Nonnull TransactionHandle handle) {
        if (handle instanceof TransactionHandleImpl) {
            var imp = (TransactionHandleImpl) handle;
            var builder = KvsTransaction.Handle.newBuilder().setSystemId(imp.getSystemId());
            return builder.build();
        }
        throw new AssertionError(); // may not occur
    }

    @Override
    public FutureResponse<TransactionHandle> send(@Nonnull KvsRequest.Begin request) throws IOException {
        return FutureResponse.returns(new TransactionHandleImpl(systemId++));
    }

    @Override
    public FutureResponse<Void> send(@Nonnull KvsRequest.Commit request) throws IOException {
        return FutureResponse.returns(null);
    }

    @Override
    public FutureResponse<GetResult> send(@Nonnull KvsRequest.Get request) throws IOException {
        var records = new LinkedList<KvsData.Record>();
        for (int i = 0; i < request.getKeysCount(); i++) {
            records.add(recBuilder.makeKvsDataRecord());
        }
        var result = new GetResultImpl(records);
        return FutureResponse.returns(result);
    }

    @Override
    public FutureResponse<PutResult> send(@Nonnull KvsRequest.Put request) throws IOException {
        var result = new PutResultImpl(request.getRecordsCount());
        return FutureResponse.returns(result);
    }

    @Override
    public FutureResponse<RemoveResult> send(@Nonnull KvsRequest.Remove request) throws IOException {
        var result = new RemoveResultImpl(request.getKeysCount());
        return FutureResponse.returns(result);
    }
}
