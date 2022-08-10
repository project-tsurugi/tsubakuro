package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Response;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class SimpleResponse implements Response {

    private final ByteBuffer main;

    private final ByteBuffer sub;

    private final Wire wire;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param sub the sub response data
     */
    public SimpleResponse(ByteBuffer main, ByteBuffer sub, Wire wire) {
        this.main = main;
        this.sub = sub;
        this.wire = wire;
    }

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param subMap map of sub response ID and its data
     */
    public SimpleResponse(ByteBuffer main) {
        this(main, null, null);
    }

    @Override
    public boolean isMainResponseReady() {
        return true;
    }

    @Override
    public ByteBuffer waitForMainResponse() {
        if (Objects.nonNull(main)) {
            return main;
        }
        try {
            return wire.response(null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) {
        return waitForMainResponse();
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
        return new SimpleResponse(null, null, wire);
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
