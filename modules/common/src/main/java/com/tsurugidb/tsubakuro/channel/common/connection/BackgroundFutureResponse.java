package com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Lang;

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

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean gotton = new AtomicBoolean();

    private final ServerResource.CloseHandler closeHandler;

    /**
     * Creates a new instance.
     * @param delegate the decoration target
     * @param mapper the response mapper
     * @param closeHandler handles {@link #close()} was invoked
     */
    public BackgroundFutureResponse(
            @Nonnull FutureResponse<? extends Response> delegate,
            @Nonnull ResponseProcessor<? extends V> mapper,
            @Nullable ServerResource.CloseHandler closeHandler) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(mapper);
        this.delegate = delegate;
        this.mapper = mapper;
        this.closeHandler = closeHandler;
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
    public synchronized V get() throws InterruptedException, IOException, ServerException {
        if (closed.get()) {
            throw new IOException("Future for " + mapper.toString() + " is already closed");
        }
        gotton.set(true);
        latch.await();
        return result.get().get();
    }

    @Override
    public synchronized V get(long timeout, TimeUnit unit)
            throws InterruptedException, IOException, ServerException, TimeoutException {
        if (closed.get()) {
            throw new IOException("Future for " + mapper.toString() + " is already closed");
        }
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
    public synchronized void close() throws IOException, ServerException, InterruptedException {
        try {
            if (!gotton.get()) {
                var obj = get();
                if (obj instanceof ServerResource) {
                    ((ServerResource) obj).close();
                }
            }
        } finally {
            if (closeHandler != null) {
                Lang.suppress(
                        e -> LOG.warn("error occurred while collecting garbage", e),
                        () -> closeHandler.onClosed(this));
            }
            delegate.close();
            closed.set(true);
        }
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
