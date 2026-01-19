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
package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * StreamConnectorImpl type.
 */
public final class StreamConnectorImpl implements Connector {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConnectorImpl.class);

    /** The default port number for the stream connector. */
    public static final int DEFAULT_PORT = 12345;

    private final String hostname;
    private final int port;

    /**
     * Creates a new instance.
     * @param hostname the hostname to connect
     * @param port the port number
     */
    public StreamConnectorImpl(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public FutureResponse<Wire> connect(@Nonnull ClientInformation clientInformation) throws IOException {
        LOG.trace("will connect to {}:{}", hostname, port); //$NON-NLS-1$
        var streamLink = new StreamLink(hostname, port);
        var wireImpl = new WireImpl(streamLink);
        return new FutureStreamWireImpl(streamLink, wireImpl, clientInformation);
    }
}
