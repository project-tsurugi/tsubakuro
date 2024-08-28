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
