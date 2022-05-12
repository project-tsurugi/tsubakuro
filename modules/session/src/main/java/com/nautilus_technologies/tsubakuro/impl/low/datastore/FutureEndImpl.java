package com.nautilus_technologies.tsubakuro.impl.low.datastore;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tateyama.proto.DatastoreResponseProtos;
import com.nautilus_technologies.tsubakuro.channel.common.FutureInputStream;

/**
 * FutureEndImpl type.
 */
public class FutureEndImpl implements FutureResponse<Void> {
    FutureInputStream futureInputStream;

    public FutureEndImpl(FutureInputStream futureInputStream) {
        this.futureInputStream = futureInputStream;
    }

    @Override
    public Void get() throws IOException, ServerException, InterruptedException {
        try {
            var response = DatastoreResponseProtos.BackupEnd.parseDelimitedFrom(futureInputStream.get());

            if (DatastoreResponseProtos.BackupEnd.ResultCase.SUCCESS.equals(response.getResultCase())) {
                return null;
            }
            throw new IOException(response.getUnknownError().getMessage());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: SessionWireImpl.receive()", e);
        }
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        // FIXME: impl
        return get();
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        // FIXME: impl
    }
}
