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
package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.testing.ResultSetWireMock;
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

    private ByteBuffer resultSetData;

    private List<? extends BlobInfo> blobList = null;

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

    @Override
    public FutureResponse<? extends Response> send(int serviceId, ByteBuffer payload, List<? extends BlobInfo> blobs) throws IOException {
        blobList = blobs;
        return send(serviceId, payload);
    }

    @Override
    public FutureResponse<? extends Response> send(int serviceId, byte[] payload, List<? extends BlobInfo> blobs) throws IOException {
        blobList = blobs;
        return send(serviceId, ByteBuffer.wrap(payload), blobs);
    }

    /**
     * Add a {@link RequestHandler} to the handler queue.
     * @param handler the request handler
     * @return this
     */
    public MockWire next(RequestHandler handler) {
        Objects.requireNonNull(handler);
        handlers.add(handler);
        return this;
    }

    /**
     * Add a {@link RequestHandler} that activates when there is no more handlers.
     * @param handler the request handler
     * @return this
     */
    public MockWire otherwise(RequestHandler handler) {
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

    List<? extends BlobInfo> blobs() {
        return blobList;
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
