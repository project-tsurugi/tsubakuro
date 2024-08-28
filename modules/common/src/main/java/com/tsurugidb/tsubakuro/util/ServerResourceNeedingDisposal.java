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

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Represents a resource which is on the server and needs request to dispose it for the server.
 */
public interface ServerResourceNeedingDisposal extends ServerResource {
    @Override
    default void setCloseTimeout(@Nullable Timeout t) {
        if (t != null) {
            setCloseTimeout(t.value(), t.unit());
        }
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    default void setCloseTimeout(long timeout, TimeUnit unit) {
        return;
    }
}
