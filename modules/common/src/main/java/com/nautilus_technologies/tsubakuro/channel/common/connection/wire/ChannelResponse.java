package com.nautilus_technologies.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import javax.annotation.Nonnull;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {

    private static final int SLEEP_UNIT = 10;  // 10mS per sleep
    private final Wire wire;
    private final AtomicBoolean queryMode = new AtomicBoolean();
    private final AtomicReference<ResponseWireHandle> handle = new AtomicReference<>();
    private final AtomicReference<ByteBuffer> main = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Lock lock = new ReentrantLock();
    private final Condition noSet = lock.newCondition();

    /**
     * Creates a new instance with a Wire
     * @param wire the Wire from which a main response will come
     */
    public ChannelResponse(@Nonnull Wire wire) {
        Objects.requireNonNull(wire);
        this.wire = wire;
        queryMode.setPlain(false);
    }

    /**
     * Creates a new instance, without any attached channel.
     * @param main the main response data
     */
    public ChannelResponse(@Nonnull ByteBuffer main) {
        this.main.set(main);
        this.wire = null;
    }

    @Override
    public boolean isMainResponseReady() {
        return Objects.nonNull(handle) || Objects.nonNull(main);
    }

    @Override
    public ByteBuffer waitForMainResponse() throws IOException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }

        lock.lock();
        try {
            while (Objects.isNull(handle.get())) {
                noSet.await();
            }
            main.set(wire.response(handle.get()));
            return main.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }

        lock.lock();
        try {
            while (Objects.isNull(handle.get())) {
                noSet.await();
            }
            main.set(wire.response(handle.get(), timeout, unit));
            return main.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    /**
     * @implNote Either this method or setResponseHandle() can be called first.
     */
    @Override
    public void setResultSetMode() {
        lock.lock();
        try {
            queryMode.set(true);
            if (Objects.nonNull(handle.get())) {
                wire.setResultSetMode(handle.get());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @implNote It must be called before release() is called.
     */
    @Override
    public Response duplicate() {
        lock.lock();
        try {
            ChannelResponse channelResponse = new ChannelResponse(wire);
            channelResponse.setResponseHandle(handle.get());
            return channelResponse;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release() throws IOException {
        lock.lock();
        try {
            if (Objects.nonNull(handle.get())) {
                wire.release(handle.get());
                handle.set(null);
            }
        } finally {
            lock.unlock();
        }
    }

    public void setResponseHandle(ResponseWireHandle h) {
        lock.lock();
        try {
            handle.set(h);
            noSet.signal();
            if (queryMode.get()) {
                wire.setResultSetMode(handle.get());
            }
        } finally {
            lock.unlock();
        }
    }
}
