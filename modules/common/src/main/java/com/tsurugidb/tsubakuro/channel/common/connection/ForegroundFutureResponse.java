package com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * A {@link FutureResponse} that converts {@link Response} into specific type in foreground.
 * @param <V> the specified response type
 */
public class ForegroundFutureResponse<V> implements FutureResponse<V> {  // FIXME remove public
    private static final int POLL_INTERVAL = 1000; // in mS

    static final Logger LOG = LoggerFactory.getLogger(ForegroundFutureResponse.class);

    private final FutureResponse<? extends Response> delegate;

    private final ResponseProcessor<? extends V> mapper;

    private final AtomicReference<Response> unprocessed = new AtomicReference<>();

    private final AtomicReference<V> result = new AtomicReference<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean gotton = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param delegate the decoration target
     * @param mapper the response mapper
     */
    public ForegroundFutureResponse(
            @Nonnull FutureResponse<? extends Response> delegate,
            @Nonnull ResponseProcessor<? extends V> mapper) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(mapper);
        this.delegate = delegate;
        this.mapper = mapper;
    }

    @Override
    public synchronized V get() throws InterruptedException, IOException, ServerException {
        if (closed.get()) {
            throw new IOException("Future for " + mapper.toString() + " is already closed");
        }
        gotton.set(true);
        var mapped = result.get();
        if (mapped != null) {
            return mapped;
        }
        return processResult(getInternal(), Timeout.DISABLED);
    }

    @Override
    public synchronized V get(long timeout, TimeUnit unit)
            throws InterruptedException, IOException, ServerException, TimeoutException {
        if (closed.get()) {
            throw new IOException("Future for " + mapper.toString() + " is already closed");
        }
        gotton.set(true);
        var mapped = result.get();
        if (mapped != null) {
            return mapped;
        }
        return processResult(getInternal(timeout, unit), new Timeout(timeout, unit, Timeout.Policy.ERROR));
    }

    private Owner<Response> getInternal() throws InterruptedException, IOException, ServerException {
        if (!mapper.isMainResponseRequired()) {
            return Owner.of(delegate.get());
        }
        try (Owner<Response> response = Owner.of(delegate.get())) {
            response.get().waitForMainResponse();
            return response.move();
        }
    }

    private Owner<Response> getInternal(long timeout, TimeUnit unit)
            throws InterruptedException, IOException, ServerException, TimeoutException {
        if (!mapper.isMainResponseRequired()) {
            return Owner.of(delegate.get(timeout, unit));
        }
        long timeoutMillis = Math.max(unit.toMillis(timeout), 2);
        long startTime = System.currentTimeMillis();
        try (Owner<Response> response = Owner.of(delegate.get(timeoutMillis, TimeUnit.MILLISECONDS))) {
            try {
                timeoutMillis = Math.max(timeoutMillis - (System.currentTimeMillis() - startTime), 2);
                response.get().waitForMainResponse(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                unprocessed.set(response.release());
                throw new ResponseTimeoutException(e);
            }
            unprocessed.set(null);
            return response.move();
        }
    }

    private V processResult(Owner<Response> response, Timeout timeout) throws IOException, ServerException, InterruptedException {
//        assert response != null;    // comment out in order to prevent SpotBugs violation
        try (response) {
            V mapped;
            synchronized (this) {
                mapped = result.get();
                if (mapped != null) {
                    return mapped;
                }
                LOG.trace("mapping response: {}", response.get()); //$NON-NLS-1$
                mapped = mapper.process(response.get(), timeout);
                LOG.trace("response mapped: {}", mapped); //$NON-NLS-1$
                result.set(mapped);
            }
            // don't close the original response only if mapping was succeeded
            response.release();
            return mapped;
        }
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public synchronized void close() throws IOException, ServerException, InterruptedException {
        try {
            if (!gotton.getAndSet(true)) {
                delegate.get().cancel();
                var obj = get();
                if (obj instanceof ServerResource) {
                    ((ServerResource) obj).close();
                }
            }
        } finally {
            Owner.close(unprocessed.getAndSet(null));
            delegate.close();
            closed.set(true);
        }
    }

    @Override
    public String toString() {
        return String.valueOf(delegate);
    }
}
