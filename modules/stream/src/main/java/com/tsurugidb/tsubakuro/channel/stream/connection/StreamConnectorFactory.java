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
        if (Objects.isNull(host)) {
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
