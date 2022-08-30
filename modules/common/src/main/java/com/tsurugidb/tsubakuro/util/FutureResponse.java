package com.tsurugidb.tsubakuro.util;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * An interface that provides asynchronous response data,
 * and it may be canceled before the underlying task was done.
 * @param <V> the result value type
 */
public interface FutureResponse<V> extends ServerResource {

    /**
     * Returns whether or not the response has already been received.
     * @return {@code true} if the response has already been received - {@link #get()} will return
     *      the response without blocking, or {@code false} otherwise
     */
    boolean isDone();

    /**
     * Retrieves the result value, or wait until response has been received.
     * <p>
     * After the response has been retrieved, {@link #close()} will never dispose the corresponding server resources.
     * If the response object holds any server resources, please dispose it.
     * </p>
     * @return the response
     * @throws IOException if exception was occurred while communicating to the server
     * @throws ServerException if exception was occurred while processing the request in the server
     * @throws InterruptedException if interrupted from other threads while waiting for response
     * @see #await()
     */
    V get() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves the response, or wait until response has been received.
     * <p>
     * After the response has been retrieved, {@link #close()} will never dispose the corresponding server resources.
     * If the response object holds any server resources, please dispose it.
     * </p>
     * @param timeout the maximum time to wait
     * @param unit the time unit of {@code timeout}
     * @return the response
     * @throws IOException if exception was occurred while communicating to the server
     * @throws ServerException if exception was occurred while processing the request in the server
     * @throws InterruptedException if interrupted from other threads while waiting for response
     * @throws TimeoutException if the wait time out
     * @see #await()
     */
    V get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException;

    /**
     * Retrieves the result value, or wait until response has been received.
     * <p>
     * Even if this operation was failed, underlying server resource will be disposed.
     * Or to keep such the server resources, please call {@link #get()} instead.
     * </p>
     * <p>
     * Developers should use this method instead of {@link #get()}, unless there is some special reason.
     * This will avoid like as the following ugly code
<pre>
try (
    FutureResponse<Resource> future = send(...);
    Resource resource = future.get();
) {
    ...
}
</pre>
     * Using {@link #await()}, it can be rewritten as follows:
<pre>
try (Resource resource = send(...).await()) {
    ...
}
</pre>
     * </p>
     * @return the response
     * @throws IOException if exception was occurred while communicating to the server
     * @throws ServerException if exception was occurred while processing the request in the server
     * @throws InterruptedException if interrupted from other threads while waiting for response
     * @see #get()
     */
    default V await() throws IOException, ServerException, InterruptedException {
        try (var self = this) {
            return self.get();
        }
    }

    /**
     * Disposes the corresponding server resources if it is required.
     * @throws IOException if I/O error was occurred while disposing the corresponding server resources
     * @throws ServerException if error was occurred while disposing the corresponding server resource
     * @throws InterruptedException if interrupted from other threads while requesting cancel
     */
    @Override
    void close() throws IOException, ServerException, InterruptedException;

    /**
     * Returns {@link java.util.concurrent.Future} view of this object.
     * @return the {@link java.util.concurrent.Future} view
     */
    default Future<V> asFuture() {
        return new Future<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return FutureResponse.this.isDone();
            }

            @Override
            public V get() throws InterruptedException, ExecutionException {
                try {
                    return FutureResponse.this.get();
                } catch (IOException | ServerException | RuntimeException e) {
                    throw new ExecutionException(e);
                }
            }

            @Override
            public V get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                try {
                    return FutureResponse.this.get(timeout, unit);
                } catch (IOException | ServerException | RuntimeException e) {
                    throw new ExecutionException(e);
                }
            }
        };
    }

    /**
     * Returns {@link FutureResponse} which just returns the value.
     * @param <V> the value type
     * @param value the response value
     * @return the wrapped object
     */
    static <V> FutureResponse<V> returns(@Nullable V value) {
        return new FutureResponse<>() {
            @Override
            public boolean isDone() {
                return true;
            }
            @Override
            public V get() {
                return value;
            }
            @Override
            public V get(long timeout, TimeUnit unit) {
                return get();
            }
            @Override
            public void close() throws IOException, InterruptedException {
                // do nothing
            }
        };
    }

    /**
     * Returns {@link FutureResponse} which just throws the given exception.
     * @param <V> the value type
     * @param exception the exception to throw from {@link #get()}
     * @return the wrapped object
     */
    static <V> FutureResponse<V> raises(@Nonnull ServerException exception) {
        Objects.requireNonNull(exception);
        return new FutureResponse<>() {
            @Override
            public boolean isDone() {
                return true;
            }
            @Override
            public V get() throws ServerException {
                throw exception;
            }
            @Override
            public V get(long timeout, TimeUnit unit) throws ServerException {
                throw exception;
            }
            @Override
            public void close() throws IOException, InterruptedException {
                // do nothing
            }
        };
    }

    /**
     * Returns {@link FutureResponse} which just returns the value.
     * @param <V> the value type
     * @param owner the value with its ownership
     * @return the wrapped object
     */
    static <V extends ServerResource> FutureResponse<V> wrap(@Nonnull Owner<? extends V> owner) {
        Objects.requireNonNull(owner);
        return new FutureResponse<>() {
            private final AtomicReference<V> provided = new AtomicReference<>();
            @Override
            public boolean isDone() {
                return true;
            }
            @Override
            public V get() {
                V r = provided.get();
                if (r != null) {
                    return r;
                }
                r = owner.release();
                provided.compareAndSet(null, r);
                return provided.get();
            }
            @Override
            public V get(long timeout, TimeUnit unit) {
                return get();
            }
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                owner.close();
            }
        };
    }
}
