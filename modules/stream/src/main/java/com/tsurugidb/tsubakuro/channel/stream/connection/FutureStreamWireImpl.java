package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureStreamWireImpl type.
 */
public class FutureStreamWireImpl implements FutureResponse<Wire> {

    private final StreamLink streamLink;
    private final WireImpl wireImpl;
    private final FutureResponse<Long> futureSessionID;
    private final AtomicBoolean gotton = new AtomicBoolean();

    FutureStreamWireImpl(StreamLink streamLink, WireImpl wireImpl, FutureResponse<Long> futureSessionID) {
        this.streamLink = streamLink;
        this.wireImpl = wireImpl;
        this.futureSessionID = futureSessionID;
    }

    @Override
    public Wire get() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            try {
                wireImpl.setSessionID(futureSessionID.get());
                return wireImpl;
            } catch (IOException | ServerException | InterruptedException e) {
                gotton.set(false);
                throw e;
            }
        }
        throw new IOException("FutureStreamWireImpl already closed.");
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        if (!gotton.getAndSet(true)) {
            try {
                wireImpl.setSessionID(futureSessionID.get(timeout, unit));
                return wireImpl;
            } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
                gotton.set(false);
                throw e;
            }
        }
        throw new IOException("FutureStreamWireImpl already closed.");
    }

    @Override
    public boolean isDone() {
        // FIXME: return status
        return false;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            futureSessionID.close();
            streamLink.closeWithoutGet();
            wireImpl.closeWithoutGet();
        }
    }
}
