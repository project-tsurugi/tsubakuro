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
package com.tsurugidb.tsubakuro.util;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Utilities for {@link Future}.
 */
public final class Futures {

    /**
     * Returns a {@link Future} which just returns the given value.
     * @param <V> the value type
     * @param value the result value
     * @return a wrapped future
     */
    public static <V> Future<V> returns(V value) {
        return new Future<>() {
            @Override
            public boolean isCancelled() {
                return false;
            }
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
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }
            @Override
            public String toString() {
                return String.valueOf(value);
            }
        };
    }

    private Futures() {
        throw new AssertionError();
    }
}
