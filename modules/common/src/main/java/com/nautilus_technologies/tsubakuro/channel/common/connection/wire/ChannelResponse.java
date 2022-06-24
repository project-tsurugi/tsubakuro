package com.nautilus_technologies.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {

    private final Wire wire;
    private ResponseWireHandle handle;
    private boolean queryMode;
    private ByteBuffer main;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance with a Wire
     * @param wire the Wire from which a main response will come
     */
    public ChannelResponse(@Nonnull Wire wire) {
        Objects.requireNonNull(wire);
        this.wire = wire;
        this.handle = null;
        this.queryMode = false;
        main = null;
    }

    /**
     * Creates a new instance, without any attached channel.
     * @param main the main response data
     */
    public ChannelResponse(@Nonnull ByteBuffer main) {
        this.main = main;
        this.wire = null;
    }

    @Override
    public synchronized boolean isMainResponseReady() {
        return Objects.nonNull(handle) || Objects.nonNull(main);
    }

    @Override
    public synchronized ByteBuffer waitForMainResponse() throws IOException {
        if (Objects.nonNull(main)) {
            return main;
        }
        if (isMainResponseReady()) {
            main = wire.response(handle);
            return main;
        }
        throw new IOException("response box is not available");  // FIXME arch. mismatch??
    }

    @Override
    public synchronized ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(main)) {
            return main;
        }
        if (isMainResponseReady()) {
            main = wire.response(handle, timeout, unit);
            return main;
        }
        throw new IOException("response box is not available");  // FIXME arch. mismatch??
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    @Override
    public synchronized void setQueryMode() {
        queryMode = true;
        if (Objects.nonNull(handle)) {
            wire.setQueryMode(handle);
        }
    }

    @Override
    public synchronized void release() {
        if (Objects.nonNull(handle)) {
            wire.release(handle);
            handle = null;
        }
    }

    @Override
    public synchronized ResponseWireHandle responseWireHandle() {
        return handle;
    }

    public synchronized void setResponseHandle(ResponseWireHandle h) {
        handle = h;
        if (queryMode) {
            wire.setQueryMode(handle);
        }
    }
}
