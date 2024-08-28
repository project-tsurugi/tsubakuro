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

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Represents a resource which is on the server or corresponding to it.
 * You must {@link #close()} after use this object.
 */
public interface ServerResource extends AutoCloseable {

    /**
     * Sets close timeout.
     * This is only effective if this resource considers close timeout.
     * @param timeout the timeout setting
     */
    default void setCloseTimeout(@Nonnull Timeout timeout) {
        return;
    }

    @Override
    void close() throws ServerException, IOException, InterruptedException;

    /**
     * Handles closed {@link ServerResource}.
     */
    @FunctionalInterface
    public interface CloseHandler {
        /**
         * Handles closed {@link ServerResource}.
         * @param resource the resource that {@link ServerResource#close()} was invoked
         */
        void onClosed(@Nonnull ServerResource resource);
    }
}
