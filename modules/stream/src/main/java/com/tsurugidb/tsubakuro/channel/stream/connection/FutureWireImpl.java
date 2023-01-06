package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureWireImpl type.
 */
public class FutureWireImpl implements FutureResponse<Wire> {

    StreamLink streamLink;
    private final AtomicBoolean gotton = new AtomicBoolean();

    FutureWireImpl(StreamLink streamLink) {
        this.streamLink = streamLink;
    }

    @Override
    public Wire get() throws IOException {
        return get(0, null);  // No timeout
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws IOException {
        try {
            var message = streamLink.helloResponse(timeout, unit);
            if (message.getInfo() == StreamLink.RESPONSE_SESSION_HELLO_OK) {
                gotton.set(true);
                return new WireImpl(streamLink, Long.parseLong(message.getString()));
            }
            throw new IOException("the server has declined the connection request");
        } catch (TimeoutException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isDone() {
        // FIXME: return status
        return false;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            var wire = get();
            wire.close();
        }
    }
}
