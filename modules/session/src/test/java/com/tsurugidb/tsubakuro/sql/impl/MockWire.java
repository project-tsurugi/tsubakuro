package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.testing.ResultSetWireMock;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.mock.MockError;

/**
 * Mock implementation of {@link Wire}.
 */
public class MockWire implements Wire {

    private final Queue<RequestHandlerForSql> handlers = new ConcurrentLinkedQueue<>();

    private final AtomicReference<RequestHandlerForSql> defaultHandler = new AtomicReference<>(new RequestHandlerForSql() {
        @Override
        public Response handle(int serviceId, ByteBuffer payload) {
            throw new MockError("no more handlers"); //$NON-NLS-1$
        }
    });

    private ByteBuffer resultSetData;

    @Override
    public FutureResponse<Response> send(int serviceId, ByteBuffer payload) throws IOException {
        var next = handlers.poll();
        if (next == null) {
            next = defaultHandler.get();
        }
        try {
            var response = next.handle(serviceId, payload);
            resultSetData = ((SimpleResponse) response).getRelation();
            return FutureResponse.wrap(Owner.of(response));
        } catch (ServerException e) {
            return FutureResponse.raises(e);
        }
    }

    @Override
    public FutureResponse<Response> send(int serviceId, byte[] payload) throws IOException {
        return send(serviceId, ByteBuffer.wrap(payload));
    }

    /**
     * Add a {@link RequestHandlerForSql} to the handler queue.
     * @param handler the request handler
     * @return this
     */
    public MockWire next(RequestHandlerForSql handler) {
        Objects.requireNonNull(handler);
        handlers.add(handler);
        return this;
    }

    /**
     * Add a {@link RequestHandlerForSql} that activates when there is no more handlers.
     * @param handler the request handler
     * @return this
     */
    public MockWire otherwise(RequestHandlerForSql handler) {
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

    @Override
    public void close() throws IOException, InterruptedException {
        handlers.clear();
    }

    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        if (resultSetData != null) {
            return new ResultSetWireMock(resultSetData);
        }
        return null;
    }
}
