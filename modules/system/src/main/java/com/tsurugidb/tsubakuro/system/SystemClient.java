/*
 * Copyright 2025-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.system;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.system.proto.SystemResponse;
import com.tsurugidb.tsubakuro.client.ServiceClient;
import com.tsurugidb.tsubakuro.client.ServiceMessageVersion;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.system.impl.SystemClientImpl;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A system service client.
 * @see #attach(Session)
 * @since 1.13.0
 */
@ServiceMessageVersion(
        service = SystemClient.SERVICE_SYMBOLIC_ID,
        major = SystemClient.SERVICE_MESSAGE_VERSION_MAJOR,
        minor = SystemClient.SERVICE_MESSAGE_VERSION_MINOR)
public interface SystemClient extends ServerResource, ServiceClient {

    /**
     * The symbolic ID of the destination service.
    */
    String SERVICE_SYMBOLIC_ID = "system";

    /**
     * The major service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MINOR = 0;

    /**
     * Attaches to the system service in the current session.
     * @param session the current session
     * @return the system service client
     */
    static SystemClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return SystemClientImpl.attach(session);
    }

    /**
     * Get the system information of the database to which the session is connected.
     * @return a future of SystemResponse.SystemInfo message
     * @throws IOException if I/O error occurred while sending request
     */
    default FutureResponse<SystemResponse.SystemInfo> getSystemInfo() throws IOException {
        throw new UnsupportedOperationException();
    }
}
