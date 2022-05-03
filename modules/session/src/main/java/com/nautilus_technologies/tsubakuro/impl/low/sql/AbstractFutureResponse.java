package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

abstract class AbstractFutureResponse<V> implements FutureResponse<V> {

    private final AtomicBoolean isDone = new AtomicBoolean(false);

    @Override
    public V get() throws IOException, ServerException, InterruptedException {
        var result = getInternal();
        isDone.set(true);
        return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        var result = getInternal(timeout, unit);
        isDone.set(true);
        return result;
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    protected abstract V getInternal() throws IOException, ServerException, InterruptedException;

    protected abstract V getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException;
}
