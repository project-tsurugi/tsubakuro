package  com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link ConnectorFactory} for testing.
 */
public class TestingConnectorFactory implements ConnectorFactory {

    @Override
    public Optional<Connector> tryCreate(URI endpoint) {
        if (Objects.equals(endpoint.getScheme(), "testing")) {
            return Optional.of(new TestingConnector(endpoint));
        }
        return Optional.empty();
    }

}
