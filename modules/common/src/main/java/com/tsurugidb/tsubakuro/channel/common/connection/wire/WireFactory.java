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

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.exception.ConnectionException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Provides {@link Wire} object for individual URIs.
 */
@ThreadSafe
public interface WireFactory {

    /**
     * Returns whether or not this recognizes the given end-point URI.
     * @param endpoint the target end-point URI
     * @return {@code true} if this recognizes the given end-point URI,
     *      otherwise {@code false}
     */
    boolean accepts(URI endpoint);

    /**
     * Establishes a new connection to the server using its end-point URI.
     * @param endpoint the target end-point URI
     * @param credential credential information to connect to the server
     * @return future of a wire object which represents a connection to the target server
     * @throws ConnectionException if connection was failed
     * @see #accepts(URI)
     */
    FutureResponse<? extends Wire> create(URI endpoint, Credential credential) throws ConnectionException;
}
