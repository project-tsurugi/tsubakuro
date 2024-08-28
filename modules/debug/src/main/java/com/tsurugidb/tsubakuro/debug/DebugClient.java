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

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.client.ServiceClient;
import com.tsurugidb.tsubakuro.client.ServiceMessageVersion;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.debug.impl.DebugClientImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A debugging service client.
 */
@ServiceMessageVersion(
        service = DebugClient.SERVICE_SYMBOLIC_ID,
        major = DebugClient.SERVICE_MESSAGE_VERSION_MAJOR,
        minor = DebugClient.SERVICE_MESSAGE_VERSION_MINOR)
public interface DebugClient extends ServerResource, ServiceClient {

    /**
     * The symbolic ID of the destination service.
    */
    String SERVICE_SYMBOLIC_ID = "debug";

    /**
     * The major service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MINOR = 0;

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    static DebugClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return DebugClientImpl.attach(session);
    }

    /**
     * Requests to output a log record on the server side.
     * @param message the log message
     * @return the future response of the request
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> logging(@Nonnull String message) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests to output a log record on the server side.
     * @param level the log level
     * @param message the log message
     * @return the future response of the request
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> logging(@Nonnull LogLevel level, @Nonnull String message) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Disposes the underlying server resources.
     * Note that, this never closes the underlying {@link Session}.
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
