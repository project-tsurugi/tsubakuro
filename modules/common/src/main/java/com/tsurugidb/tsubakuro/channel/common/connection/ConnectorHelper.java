package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.net.URI;
import java.text.MessageFormat;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectorHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorHelper.class);

    static Connector create(URI endpoint) {
        Objects.requireNonNull(endpoint);
        LOG.trace("creating connector: {}", endpoint); //$NON-NLS-1$
        for (var factory : ServiceLoader.load(ConnectorFactory.class)) {
            var connectorOpt = factory.tryCreate(endpoint);
            if (connectorOpt.isPresent()) {
                LOG.trace("found ConnectorFactory: {} - {}", factory, endpoint); //$NON-NLS-1$
                return connectorOpt.get();
            }
        }
        throw new NoSuchElementException(MessageFormat.format(
                "suitable connector is not found: {0}",
                endpoint));
    }

    private ConnectorHelper() {
        throw new AssertionError();
    }
}
