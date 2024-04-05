package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

abstract class AbstractFutureResponse<V> implements FutureResponse<V> {

    private final AtomicReference<V> getInternalResult = new AtomicReference<>();
    private final Lock lock = new ReentrantLock();

    @Override
    public V get() throws IOException, ServerException, InterruptedException {
        while (true) {
            var result = getInternalResult.get();
            if (result != null) {
                return result;
            }
            lock.lock();
            try {
                getInternalResult.set(getInternal());
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        while (true) {
            var result = getInternalResult.get();
            if (result != null) {
                return result;
            }
            if (lock.tryLock(timeout, unit)) {
                try {
                    getInternalResult.set(getInternal(timeout, unit));
                } finally {
                    lock.unlock();
                }
            } else {
                throw new TimeoutException("getInternal() by another thread has not returned within the specifined time");
            }
        }
    }

    @Override
    public boolean isDone() {
        return getInternalResult.get() != null;
    }

    protected abstract V getInternal() throws IOException, ServerException, InterruptedException;

    protected abstract V getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException;
}
