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

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Maps future value of  {@link FutureResponse}.
 * @param <T> the input type
 * @param <V> the result type
 */
public final class MappedFutureResponse<T extends ServerResource, V> implements FutureResponse<V> {

    static final Logger LOG = LoggerFactory.getLogger(MappedFutureResponse.class);

    private final FutureResponse<? extends T> delegate;

    private final Function<? super T, ? extends V> mapper;

    private final AtomicReference<V> result = new AtomicReference<>();

    /**
     * Creates a new instance.
     * @param delegate the decoration target
     * @param mapper the response mapper
     */
    public MappedFutureResponse(
            @Nonnull FutureResponse<? extends T> delegate,
            @Nonnull Function<? super T, ? extends V> mapper) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(mapper);
        this.delegate = delegate;
        this.mapper = mapper;
    }

    @Override
    public V get() throws InterruptedException, IOException, ServerException {
        var mapped = result.get();
        if (mapped != null) {
            return mapped;
        }
        return doApply(Owner.of(delegate.get()));
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, IOException, ServerException, TimeoutException {
        var mapped = result.get();
        if (mapped != null) {
            return mapped;
        }
        return doApply(Owner.of(delegate.get(timeout, unit)));
    }

    private V doApply(Owner<T> response) throws IOException, ServerException, InterruptedException {
        try (response) {
            var mapped = mapper.apply(response.get());
            result.compareAndSet(null, mapped);
            response.release();
            return result.get();
        }
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        delegate.close();
    }

    @Override
    public String toString() {
        return String.valueOf(delegate);
    }
}
