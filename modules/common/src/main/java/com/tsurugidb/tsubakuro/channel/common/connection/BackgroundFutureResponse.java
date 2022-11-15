package com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.reflect.InvocationTargetException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * A {@link FutureResponse} that converts {@link Response} into specific type in background.
 * The runtime should invoke {@link #run()} in background.
 * @param <V> the specified response type
 * @see ForegroundFutureResponse
 */
public class BackgroundFutureResponse<V> implements FutureResponse<V>, Runnable {  // FIXME remove public

    static final Logger LOG = LoggerFactory.getLogger(BackgroundFutureResponse.class);

    private final FutureResponse<? extends Response> delegate;

    private final ResponseProcessor<? extends V> mapper;

    private final CountDownLatch latch = new CountDownLatch(1);

    private final AtomicReference<Result<V>> result = new AtomicReference<>();

    private final AtomicBoolean gotton = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param delegate the decoration target
     * @param mapper the response mapper
     */
    public BackgroundFutureResponse(
            @Nonnull FutureResponse<? extends Response> delegate,
            @Nonnull ResponseProcessor<? extends V> mapper) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(mapper);
        this.delegate = delegate;
        this.mapper = mapper;
    }

    @Override
    public void run() {
        synchronized (delegate) {
            if (result.get() != null) {
                return;
            }
            try {
                V r = mapper.process(delegate.get());
                result.set(() -> {
                    return r;
                });
            } catch (IOException | ServerException | InterruptedException | RuntimeException | Error e) {
                result.set(() -> {
                    throw e;
                });
            } finally {
                latch.countDown();
            }
        }
    }

    @Override
    public V get() throws InterruptedException, IOException, ServerException {
        gotton.set(true);
        latch.await();
        return result.get().get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, IOException, ServerException, TimeoutException {
        gotton.set(true);
        if (!latch.await(timeout, unit)) {
            throw new TimeoutException("detects timeout");
        }
        return result.get().get();
    }

    @Override
    public boolean isDone() {
        return result.get() != null;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.get()) {
            var obj = get();
            var cls = obj.getClass();
            try {
                cls.getMethod("close").invoke(obj);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }
        delegate.close();
    }

    @Override
    public String toString() {
        return String.valueOf(delegate);
    }

    @FunctionalInterface
    private interface Result<V> {
        V get() throws IOException, ServerException, InterruptedException;
    }
}
