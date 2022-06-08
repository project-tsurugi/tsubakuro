package com.nautilus_technologies.tsubakuro.channel.common.wire;

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

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;

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

    @Override
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
