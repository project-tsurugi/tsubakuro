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

import com.tsurugidb.core.proto.CoreRequest;

public enum ShutdownType {
    /**
     * Waits for the ongoing requests and safely shutdown the session.
     * <p>
     * This may wait for complete the current running requests, and then shutdown this session.
     * </p>
     */
    GRACEFUL(CoreRequest.ShutdownType.GRACEFUL),

    /**
     * Cancelling the ongoing requests and safely shutdown the session.
     * <p>
     * This request will first the cancelling the ongoing requests.
     * Once each request detects the cancellation, it will discontinue the subsequent work.
     * Finally, this operation may wait for complete or cancel the requests, and then shutdown this session.
     * </p>
     */
    FORCEFUL(CoreRequest.ShutdownType.FORCEFUL);

    private final CoreRequest.ShutdownType type;

    ShutdownType(CoreRequest.ShutdownType type) {
        this.type = type;
    }

    public CoreRequest.ShutdownType type() {
        return type;
    }
}
