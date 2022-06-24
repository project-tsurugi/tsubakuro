package  com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.net.URI;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * A factory of {@link Connector} implementation.
 */
public interface ConnectorFactory {

    /**
     * Creates a new connector if the end-point URI is suitable for this.
     * @param endpoint the target end-point URI
     * @return a connector which provides a connection to the target end-point
     */
    Optional<? extends Connector> tryCreate(@Nonnull URI endpoint);

}
