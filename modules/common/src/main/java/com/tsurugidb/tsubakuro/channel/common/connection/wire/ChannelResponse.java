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

import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

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
    private final boolean streamMode;

    /**
     * Creates a new instance with a Wire
     * @param wire the Wire from which a main response will come
     */
    public ChannelResponse(@Nonnull Wire wire) {
        Objects.requireNonNull(wire);
        this.wire = wire;
        this.streamMode = false;
    }

        /**
     * Creates a new instance with a Wire
     * @param wire the Wire from which a main response will come
     */
    public ChannelResponse() {
        this.wire = null;
        this.streamMode = true;
    }

    /**
     * Creates a new instance, without any attached channel.
     * @param main the main response data
     */
    public ChannelResponse(@Nonnull ByteBuffer main) {
        this.main.set(main);
        this.wire = null;
        this.streamMode = false;
    }

    @Override
    public boolean isMainResponseReady() {
        return Objects.nonNull(handle) || Objects.nonNull(main.get());
    }

    @Override
    public ByteBuffer waitForMainResponse() throws IOException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }

        if (streamMode) {
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
        } else {
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
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }

        if (streamMode) {
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
        } else {
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
    }

    @Override
    public boolean isSecondResponseReady() {
        return Objects.nonNull(handle) || Objects.nonNull(second.get());
    }

    @Override
    public ByteBuffer waitForSecondResponse() throws IOException {
        if (Objects.nonNull(second.get())) {
            return second.get();
        }

        if (streamMode) {
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
        } else {
            lock.lock();
            try {
                second.set(wire.response(handle.get()));
                return second.get();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public ByteBuffer waitForSecondResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(second.get())) {
            return second.get();
        }

        if (streamMode) {
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
        } else {
            lock.lock();
            try {
                second.set(wire.response(handle.get(), timeout, unit));
                return second.get();
            } finally {
                lock.unlock();
            }
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
