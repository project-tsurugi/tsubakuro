/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
                result = getInternalResult.get();
                if (result != null) {
                    return result;
                }
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
                result = getInternalResult.get();
                if (result != null) {
                    return result;
                }
                try {
                    getInternalResult.set(getInternal(timeout, unit));
                } finally {
                    lock.unlock();
                }
            } else {
                throw new TimeoutException("getInternal() by another thread has not returned within the specifined time (" + timeout + " " + unit + ")");
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
