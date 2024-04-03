package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
    private final AtomicReference<Wire> result = new AtomicReference<>();
    private boolean closed = false;

    FutureStreamWireImpl(StreamLink streamLink, WireImpl wireImpl, FutureResponse<Long> futureSessionID) {
        this.streamLink = streamLink;
        this.wireImpl = wireImpl;
        this.futureSessionID = futureSessionID;
    }

    @Override
    public synchronized Wire get() throws IOException, ServerException, InterruptedException {
        var wire = result.get();
        if (wire != null) {
            return wire;
        }
        wireImpl.setSessionID(futureSessionID.get());
        result.set(wireImpl);
        return wireImpl;
    }

    @Override
    public synchronized Wire get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        var wire = result.get();
        if (wire != null) {
            return wire;
        }
        wireImpl.setSessionID(futureSessionID.get(timeout, unit));
        result.set(wireImpl);
        return wireImpl;
    }

    @Override
    public boolean isDone() {
        // FIXME: return status
        return false;
    }

    @Override
    public synchronized void close() throws IOException, ServerException, InterruptedException {
        if (!closed) {
            closed = true;
            if (result.get() == null) {
                try {
                    futureSessionID.close();
                } finally {
                    try {
                        streamLink.closeWithoutGet();
                    } finally {
                        wireImpl.closeWithoutGet();
                    }
                }
            }
        }
    }
}
