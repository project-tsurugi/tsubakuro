package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

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

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {

    private static final int SLEEP_UNIT = 10;  // 10mS per sleep
    private final AtomicReference<ByteBuffer> main = new AtomicReference<>();
    private final AtomicReference<ByteBuffer> second = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Lock lock = new ReentrantLock();
    private final Condition noSet = lock.newCondition();

    /**
     * Creates a new instance
     */
    public ChannelResponse() {
    }

    @Override
    public boolean isMainResponseReady() {
        return Objects.nonNull(main.get());
    }

    @Override
    public ByteBuffer waitForMainResponse() throws IOException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }

        lock.lock();
        try {
            while (Objects.isNull(main.get())) {
                noSet.await();
            }
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
            while (Objects.isNull(main.get())) {
                noSet.await();
            }
            return main.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isSecondResponseReady() {
        return Objects.nonNull(second.get());
    }

    @Override
    public ByteBuffer waitForSecondResponse() throws IOException {
        if (Objects.nonNull(second.get())) {
            return second.get();
        }

        lock.lock();
        try {
            while (Objects.isNull(second.get())) {
                noSet.await();
            }
            return second.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
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
            while (Objects.isNull(second.get())) {
                noSet.await();
            }
            return second.get();
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

    public void setMainResponse(@Nonnull ByteBuffer response) {
        lock.lock();
        try {
            main.set(skipFrameworkHeader(response));
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }

    public void setSecondResponse(@Nonnull ByteBuffer response) {
        lock.lock();
        try {
            second.set(skipFrameworkHeader(response));
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }

    private ByteBuffer skipFrameworkHeader(ByteBuffer response) {
        try {
            response.rewind();
            FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(response));
            return response;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;  // FIXME
    }
}
