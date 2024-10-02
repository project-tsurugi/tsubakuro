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

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * Processes the {@link Response} and returns request specific result value.
 * @param <T> the result value type
 */
public interface ResponseProcessor<T> {

    /**
     * Returns whether or not this requires {@link Response#waitForMainResponse()} is already available
     * before {@link #process(Response)} is invoked.
     * That is, if this returns {@code true},
     * {@link java.util.concurrent.Future#isDone() response.getPayload().isDone()} is always true.
     * @return {@code true} if payload is available
     */
    default boolean isMainResponseRequired() {
        return true;
    }

    /**
     * Processes the {@link Response} object and returns a request specific result value.
     * If the result value is not corresponding to the server resource,
     * you must dispose by using {@link Response#close()} before complete this method.
     * @param response the process target
     * @return the processed result
     * @throws IOException if I/O error was occurred while processing the response
     * @throws ServerException if server error was occurred while processing the response
     * @throws InterruptedException if interrupted while processing the response
     */
    T process(@Nonnull Response response) throws IOException, ServerException, InterruptedException;

    /**
     * Processes the {@link Response} object and returns a request specific result value.
     * If the result value is not corresponding to the server resource,
     * you must dispose by using {@link Response#close()} before complete this method.
     * @param response the process target
     * @param timeout the maximum time to wait the subresponse
     * @return the processed result
     * @throws IOException if I/O error was occurred while processing the response
     * @throws ServerException if server error was occurred while processing the response
     * @throws InterruptedException if interrupted while processing the response
     */
    default T process(@Nonnull Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
        return process(response);
    }

    /**
     * Returns whether or not this returns ServerResource.
     * In order to ensure fail safe nature, default method returns true.
     * @return {@code true} if this returns ServerResource
     */
    default boolean isReturnsServerResource() {
        return true;
    }
}
