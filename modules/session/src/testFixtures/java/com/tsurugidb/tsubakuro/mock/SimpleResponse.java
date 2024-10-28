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
package com.tsurugidb.tsubakuro.mock;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class SimpleResponse implements Response {

    private final ByteBuffer main;

    private final Map<String, ByteBuffer> subs;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param subMap map of sub response ID and its data
     */
    public SimpleResponse(@Nonnull ByteBuffer main, @Nonnull Map<String, ByteBuffer> subMap) {
        Objects.requireNonNull(main);
        Objects.requireNonNull(subMap);
        this.main = main;
        this.subs = new TreeMap<>();
        for (var entry : subMap.entrySet()) {
            this.subs.put(entry.getKey(), entry.getValue().duplicate());
        }
    }

    /**
     * Creates a new instance, without any attached data.
     * @param main the main response data
     */
    public SimpleResponse(@Nonnull ByteBuffer main) {
        this(main, Collections.emptyMap());
    }

    @Override
    public boolean isMainResponseReady() {
        return true;
    }

    @Override
    public ByteBuffer waitForMainResponse() {
        return main;
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) {
        return main;
    }

    /**
     * Returns the sub-response identifiers included in this response.
     * @return the sub-response identifiers
     * @throws IOException if I/O error was occurred while opening the sub-responses
     * @throws ServerException if server error was occurred while opening the sub-responses
     * @throws InterruptedException if interrupted by other threads while opening the sub-responses
     */
    public Collection<String> getSubResponseIds() throws IOException, ServerException, InterruptedException {
        return new ArrayList<>(subs.keySet());
    }

    @Override
    public InputStream openSubResponse(String id) throws IOException, ServerException, InterruptedException {
        checkOpen();
        var data = subs.remove(id);
        if (data == null) {
            throw new NoSuchElementException(id);
        }
        return new ByteBufferInputStream(data);
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("response was already closed");
        }
    }

    @Override
    public void cancel() throws IOException {
    }

    @Override
    public void close() throws IOException, InterruptedException {
        subs.clear();
        closed.set(true);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "SimpleResponse(main={0}, sub={1})",
                main.remaining(),
                subs.keySet());
    }
}
