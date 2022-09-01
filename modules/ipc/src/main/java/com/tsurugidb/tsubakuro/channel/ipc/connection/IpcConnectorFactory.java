package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.ConnectorFactory;

/**
 * An implementation of {@link ConnectorFactory} which provides instances of {@link IpcConnectorImpl}.
 *
 * This factory can handle {@code ipc:<channel-name>} style end-point URI, and will ignore other parts of it.
 */
public class IpcConnectorFactory implements ConnectorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IpcConnectorFactory.class);

    private static final String SCHEME = "ipc"; //$NON-NLS-1$

    @Override
    public Optional<IpcConnectorImpl> tryCreate(@Nonnull URI endpoint) {
        Objects.requireNonNull(endpoint);

        LOG.trace("testing whether or not URI is suitable for {} connector: '{}'", SCHEME, endpoint); //$NON-NLS-1$


        if (!Objects.equals(endpoint.getScheme(), SCHEME)) {
            LOG.trace("URI is not suitable for {} connector: '{}' (invalid scheme)", SCHEME, endpoint); //$NON-NLS-1$
            return Optional.empty();
        }

        var body = endpoint.getSchemeSpecificPart();
        if (Objects.isNull(body) || body.isEmpty()) {
            LOG.trace("URI is not suitable for {} connector: '{}' (invalid channel name)", SCHEME, endpoint); //$NON-NLS-1$
            return Optional.empty();
        }

        LOG.debug("recognized endpoit URI scheme='{}', name='{}'", SCHEME, body); //$NON-NLS-1$
        return Optional.of(new IpcConnectorImpl(body));
    }
}
