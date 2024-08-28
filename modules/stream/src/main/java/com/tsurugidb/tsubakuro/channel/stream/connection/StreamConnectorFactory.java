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

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.ConnectorFactory;

/**
 * An implementation of {@link ConnectorFactory} which provides instances of {@link StreamConnectorImpl}.
 *
 * This factory can handle {@code tcp://<host>:<port>} style end-point URI, and will ignore other parts of it.
 */
public class StreamConnectorFactory implements ConnectorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConnectorFactory.class);

    private static final String SCHEME = "tcp"; //$NON-NLS-1$

    @Override
    public Optional<StreamConnectorImpl> tryCreate(@Nonnull URI endpoint) {
        Objects.requireNonNull(endpoint);

        LOG.trace("testing whether or not URI is suitable for {} connector: '{}'", SCHEME, endpoint); //$NON-NLS-1$

        if (!Objects.equals(endpoint.getScheme(), SCHEME)) {
            LOG.trace("URI is not suitable for {} connector: '{}' (invalid scheme)", SCHEME, endpoint); //$NON-NLS-1$
            return Optional.empty();
        }

        var host = endpoint.getHost();
        if (host == null) {
            LOG.trace("URI is not suitable for {} connector: '{}' (invalid host)", SCHEME, endpoint); //$NON-NLS-1$
            return Optional.empty();
        }

        var port = endpoint.getPort();
        if (port < 0) {
            LOG.trace("URI is not suitable for {} connector: '{}' (invalid port)", SCHEME, endpoint); //$NON-NLS-1$
            return Optional.empty();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("recognized endpoit URI scheme='{}', host='{}', port={}", new Object[] { //$NON-NLS-1$
                    SCHEME,
                    host,
                    port,
            });
        }
        return Optional.of(new StreamConnectorImpl(host, port));
    }
}
