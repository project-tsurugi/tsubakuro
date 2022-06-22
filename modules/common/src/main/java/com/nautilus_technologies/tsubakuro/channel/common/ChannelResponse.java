package com.nautilus_technologies.tsubakuro.channel.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {

    private final SessionWire wire;
    private ResponseWireHandle handle;
    private boolean queryMode;
    private final ByteBuffer main;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param main the main response data
     */
    public ChannelResponse(@Nonnull SessionWire wire) {
        Objects.requireNonNull(wire);
        this.wire = wire;
        this.handle = null;
        this.queryMode = false;
        main = null;
    }

    /**
     * Creates a new instance, without any attached data.
     * @param main the main response data
     */
    public ChannelResponse(@Nonnull ByteBuffer main) {
        this.main = main;
        this.wire = null;
    }

    @Override
    public synchronized boolean isMainResponseReady() {
        return Objects.nonNull(handle);
    }

    @Override
    public synchronized ByteBuffer waitForMainResponse() throws IOException {
        if (Objects.nonNull(main)) {
            return main;
        }
        if (isMainResponseReady()) {
            return wire.response(handle);
        }
        throw new IOException("response box is not available");  // FIXME arch. mismatch??
    }

    @Override
    public synchronized ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(main)) {
            return main;
        }
        if (isMainResponseReady()) {
            return wire.response(handle, timeout, unit);
        }
        throw new IOException("response box is not available");  // FIXME arch. mismatch??
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    @Override
    public synchronized ResponseWireHandle responseWireHandle() {
        return handle;
    }

    @Override
    public synchronized void setQueryMode() {
        queryMode = true;
        if (Objects.nonNull(handle)) {
            wire.setQueryMode(handle);
        }
    }

    public synchronized void setResponseHandle(ResponseWireHandle h) {
        handle = h;
        if (queryMode) {
            wire.setQueryMode(handle);
        }
    }
}
