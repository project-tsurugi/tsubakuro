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
package com.tsurugidb.tsubakuro.debug;

import javax.annotation.Nonnull;

import com.tsurugidb.debug.proto.DebugRequest;

/**
 * Represents a kind of log level on the server side.
 */
public enum LogLevel {

    /**
     * {@code INFO} level.
     */
    INFO(DebugRequest.Logging.Level.INFO),

    /**
     * {@code WARN} level.
     */
    WARN(DebugRequest.Logging.Level.WARN),

    /**
     * {@code ERROR} level.
     */
    ERROR(DebugRequest.Logging.Level.ERROR),

    ;

    private DebugRequest.Logging.Level mapping;

    LogLevel(@Nonnull DebugRequest.Logging.Level mapping) {
        assert mapping != null;
        this.mapping = mapping;
    }

    /**
     * Returns the corresponded representation in protocol buffer.
     * @return the corresponded value
     */
    public DebugRequest.Logging.Level getMapping() {
        return mapping;
    }
}
