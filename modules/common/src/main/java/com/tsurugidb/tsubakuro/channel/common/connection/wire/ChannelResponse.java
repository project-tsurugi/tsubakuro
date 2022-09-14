package com.tsurugidb.tsubakuro.channel.common.connection.wire;

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
    private final AtomicReference<ResponseWireHandle> handle = new AtomicReference<>();
    private final AtomicReference<ByteBuffer> main = new AtomicReference<>();
    private final AtomicReference<ByteBuffer> second = new AtomicReference<>();
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
    public ByteBuffer waitForSecondResponse() throws IOException {
        if (Objects.nonNull(second.get())) {
            return second.get();
        }

        lock.lock();
        try {
            second.set(wire.response(handle.get()));
            return second.get();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer waitForSecondResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(second.get())) {
            return second.get();
        }

        lock.lock();
        try {
            second.set(wire.response(handle.get(), timeout, unit));
            return second.get();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    public void setResponseHandle(ResponseWireHandle h) {
        lock.lock();
        try {
            handle.set(h);
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }
}
