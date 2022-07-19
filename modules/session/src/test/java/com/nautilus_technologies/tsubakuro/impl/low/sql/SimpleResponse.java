package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Response;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class SimpleResponse implements Response {

    private final ByteBuffer main;

    private final ByteBuffer sub;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param sub the sub response data
     */
    public SimpleResponse(ByteBuffer main, ByteBuffer sub) {
        Objects.requireNonNull(main);
        this.main = main;
        this.sub = sub;
    }

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param subMap map of sub response ID and its data
     */
    public SimpleResponse(ByteBuffer main) {
        this(main, null);
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

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("response was already closed");
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "SimpleResponse(main={0}, sub={1})",
                main.remaining());
    }

    @Override
    public Response duplicate() {
        return new SimpleResponse(main);
    }

    @Override
    public void setResultSetMode() {
    }

    @Override
    public void release() {
    }

    public ByteBuffer getSub() {
        return sub;
    }
}
