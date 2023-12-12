package com.tsurugidb.tsubakuro.mock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

/**
 * Mock implementation of {@link Wire}.
 */
public class MockWire implements Wire {

    private final Queue<RequestHandler> handlers = new ConcurrentLinkedQueue<>();

    private final AtomicReference<RequestHandler> defaultHandler = new AtomicReference<>(new RequestHandler() {
        @Override
        public Response handle(int serviceId, ByteBuffer payload) {
            throw new MockError("no more handlers"); //$NON-NLS-1$
        }
    });

    @Override
    public FutureResponse<Response> send(int serviceId, ByteBuffer payload) throws IOException {
        var next = handlers.poll();
        if (next == null) {
            next = defaultHandler.get();
        }
        try {
            return FutureResponse.wrap(Owner.of(next.handle(serviceId, payload)));
        } catch (ServerException e) {
            return FutureResponse.raises(e);
        }
    }

    /**
     * Add a {@link RequestHandler} to the handler queue.
     * @param handler the request handler
     * @return this
     */
    public MockWire next(@Nonnull RequestHandler handler) {
        Objects.requireNonNull(handler);
        handlers.add(handler);
        return this;
    }

    /**
     * Add a {@link RequestHandler} that activates when there is no more handlers.
     * @param handler the request handler
     * @return this
     */
    public MockWire otherwise(@Nonnull RequestHandler handler) {
        Objects.requireNonNull(handler);
        handlers.add(handler);
        return this;
    }

    /**
     * Returns whether or not this object has more remaining handlers.
     * @return {@code true} if this has more remaining handlers, otherwise {@code false}
     */
    public boolean hasRemaining() {
        return !handlers.isEmpty();
    }

    /**
     * Retrieves the number of remaining handlers
     * @return number of remaining handlers
     */
    public int size() {
        return handlers.size();
    }

    @Override
    public void close() throws IOException, InterruptedException {
        handlers.clear();
    }
}
