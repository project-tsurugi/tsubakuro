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
package com.tsurugidb.tsubakuro.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.Message;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Handles request for wires and returns its response.
 */
@FunctionalInterface
public interface RequestHandler {

    /**
     * Handles a request message and returns a response for it.
     * @param serviceId the destination service ID
     * @param request the request payload
     * @return the response
     * @throws IOException if I/O error while handling the request
     * @throws ServerException if server error while handling the request
     */
    Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException;

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @param channels the sub-responses
     * @return the request handler
     */
    static RequestHandler returns(@Nonnull ByteBuffer response, @Nonnull Map<String, ByteBuffer> channels) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(channels);
        return (id, request) -> new SimpleResponse(response, channels);
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @param channels the sub-responses
     * @return the request handler
     */
    @SafeVarargs
    static RequestHandler returns(@Nonnull ByteBuffer response, @Nonnull Map.Entry<String, ByteBuffer>... channels) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(channels);
        return returns(response, Map.ofEntries(channels));
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @param channels the sub-responses
     * @return the request handler
     */
    @SafeVarargs
    static RequestHandler returns(@Nonnull byte[] response, @Nonnull Map.Entry<String, byte[]>... channels) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(channels);
        return returns(ByteBuffer.wrap(response), Arrays.stream(channels)
                .collect(Collectors.toMap(Map.Entry::getKey, it -> ByteBuffer.wrap(it.getValue()))));
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @return the request handler
     * @throws IOException if I/O error while handling the request
     */
    static RequestHandler returns(@Nonnull Message response) throws IOException {
        Objects.requireNonNull(response);
        // NOTE: only calling "response.toByteArray()" causes
        // com.google.protobuf.InvalidProtocolBufferException
        try (var buffer = new ByteArrayOutputStream()) {
            response.writeDelimitedTo(buffer);
            return returns(buffer.toByteArray());
        }
    }

    /**
     * Creates a new request handler which throws the given exception.
     * @param exception the exception object
     * @return the request handler
     */
    static RequestHandler raises(@Nonnull ServerException exception) {
        Objects.requireNonNull(exception);
        return (id, request) -> {
            throw exception;
        };
    }
}
