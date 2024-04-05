package com.tsurugidb.tsubakuro.common;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

abstract class AbstractFutureResponse<V> implements FutureResponse<V> {

    private final AtomicBoolean doingGet = new AtomicBoolean(false);
    private final AtomicReference<V> getResult = new AtomicReference<>();
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    @Override
    public V get() throws IOException, ServerException, InterruptedException {
        while (true) {
            var result = getResult.get();
            if (result != null) {
                return result;
            }
            if (!doingGet.getAndSet(true)) {
                getResult.set(getInternal());
                lock.lock();
                try {
                    doingGet.set(false);
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            } else {
                lock.lock();
                try {
                    if (!doingGet.get()) {
                        continue;
                    }
                    condition.await();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        while (true) {
            var result = getResult.get();
            if (result != null) {
                return result;
            }
            if (!doingGet.getAndSet(true)) {
                getResult.set(getInternal(timeout, unit));
                lock.lock();
                try {
                    doingGet.getAndSet(false);
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            } else {
                lock.lock();
                try {
                    if (!doingGet.get()) {
                        continue;
                    }
                    if (!condition.await(timeout, unit)) {
                        throw new TimeoutException("getInternal() by another thread has not returned within the specifined time");
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    @Override
    public boolean isDone() {
        return getResult.get() != null;
    }

    protected abstract V getInternal() throws IOException, ServerException, InterruptedException;

    protected abstract V getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException;
}
