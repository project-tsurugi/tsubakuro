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
package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.net.URI;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Connector type.
 */
public interface Connector {

    /**
     * Creates a new connector for the end-point string.
     * @param endpoint the end-point URI
     * @return the corresponded connector
     * @throws IllegalArgumentException if the end-point string is not a valid URI
     * @throws NoSuchElementException if there is no suitable connector implementation for the URI
     */
    static Connector create(@Nonnull String endpoint) {
        return ConnectorHelper.create(URI.create(endpoint));
    }

    /**
     * Creates a new connector for the end-point URI.
     * @param endpoint the end-point URI
     * @return the corresponded connector
     * @throws NoSuchElementException if there is no suitable connector implementation for the URI
     */
    static Connector create(@Nonnull URI endpoint) {
        return ConnectorHelper.create(endpoint);
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @return future session wire
     * @throws IOException connection error
     */
    default FutureResponse<Wire> connect() throws IOException {
        return connect(new ClientInformation());
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @param clientInformation the client information
     * @return future session wire
     * @throws IOException connection error
     */
    FutureResponse<Wire> connect(@Nonnull ClientInformation clientInformation) throws IOException;
}
