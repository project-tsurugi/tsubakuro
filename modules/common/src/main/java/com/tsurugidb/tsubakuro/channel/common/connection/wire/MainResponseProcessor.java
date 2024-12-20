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
package com.tsurugidb.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Processes the {@link Response#waitForMainResponse() main response} and returns request specific result value.
 * @param <T> the result value type
 */
@FunctionalInterface
public interface MainResponseProcessor<T> {

    /**
     * Processes the main response and returns a request specific result value.
     * @param payload response payload
     * @return the processed result
     * @throws IOException if I/O error was occurred while processing the response
     * @throws ServerException if server error was occurred while processing the response
     * @throws InterruptedException if interrupted while processing the response
     */
    T process(@Nonnull ByteBuffer payload) throws IOException, ServerException, InterruptedException;

    /**
     * Returns {@link ResponseProcessor} view of this object.
     * @return the corresponding {@link ResponseProcessor}
     */
    default ResponseProcessor<T> asResponseProcessor() {
        return asResponseProcessor(true);
    }

    /**
     * Returns {@link ResponseProcessor} view of this object.
     * @param returnsServerResource true if this returns ServerResource
     * @return the corresponding {@link ResponseProcessor}
     */
    default ResponseProcessor<T> asResponseProcessor(boolean returnsServerResource) {
        var self = this;
        return new ResponseProcessor<>() {

            @Override
            public boolean isMainResponseRequired() {
                return true;
            }

            @Override
            public T process(@Nonnull Response response) throws IOException, ServerException, InterruptedException {
                Objects.requireNonNull(response);
                try (response) {
                    return self.process(response.waitForMainResponse());
                }
            }

            @Override
            public String toString() {
                return String.valueOf(self);
            }

            @Override
            public boolean isReturnsServerResource() {
                return returnsServerResource;
            }
        };
    }
}
