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
import java.nio.ByteBuffer;
//import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.protobuf.Message;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

/**
 * Abstract implementation of {@link ResponseProcessor} for providing {@link ResultSet}.
 * @param <T> the response message type
 */
abstract class AbstractResultSetProcessor<T extends Message>
        implements ResponseProcessor<ResultSet>, ResultSetImpl.ResponseTester {

    private final ServerResourceHolder resources;

    protected final AtomicReference<T> cache = new AtomicReference<>();

    private final AtomicBoolean passed = new AtomicBoolean();

    AbstractResultSetProcessor(@Nonnull ServerResourceHolder resources) {
        Objects.requireNonNull(resources);
        this.resources = resources;
    }

//    private void validateMetadata(
//            Response response) throws IOException, ServerException, InterruptedException {
//        if (!response.getSubResponseIds().contains(metadataChannel)) {
//            // if the response does not contain metadata, first we check main-response
//            test(response);
//            // or else, message seems to be broken
//            throw new BrokenResponseException(MessageFormat.format(
//                    "missing result set metadata in the response: ''{0}''",
//                    metadataChannel));
//        }
//    }

//    private void validateRelationData(
//            Response response) throws IOException, ServerException, InterruptedException {
//        if (!response.getSubResponseIds().contains(relationChannel)) {
//            // if the response does not contain relation data, first we check main-response
//            test(response);
//            // or else, message seems to be broken
//            throw new BrokenResponseException(MessageFormat.format(
//                    "missing result set relation data in the response: ''{0}''",
//                    relationChannel));
//        }
//    }

    @Override
    public void test(@Nonnull Response response) throws IOException, ServerException, InterruptedException {
        Objects.requireNonNull(response);
        if (passed.get()) {
            // already passed
            return;
        }
        if (cache.get() == null) {
            var message = parse(response.waitForMainResponse());
            cache.compareAndSet(null, message);
        }
        doTest(cache.get());
        passed.set(true);
    }

    @Override
    public void test(@Nonnull Response response, long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        Objects.requireNonNull(response);
        if (passed.get()) {
            // already passed
            return;
        }
        if (cache.get() == null) {
            T message;
            if (timeout > 0) {
                message = parse(response.waitForMainResponse(timeout, unit));
            } else {
                message = parse(response.waitForMainResponse());
            }
            cache.compareAndSet(null, message);
        }
        doTest(cache.get());
        passed.set(true);
    }

    abstract T parse(@Nonnull ByteBuffer payload) throws IOException;

    abstract void doTest(@Nonnull T response) throws IOException, ServerException, InterruptedException;

    @Override
    public ResultSet process(Response response) throws IOException, ServerException, InterruptedException {
        return process(response, Timeout.DISABLED);
    }
}
