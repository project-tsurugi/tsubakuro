package com.nautilus_technologies.tsubakuro.channel.common;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.channel.common.wire.ResponseProcessor;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

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
        latch.await();
        return result.get().get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, IOException, ServerException, TimeoutException {
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
