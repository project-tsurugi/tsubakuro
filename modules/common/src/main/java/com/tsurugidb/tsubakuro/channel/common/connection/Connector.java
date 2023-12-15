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
        return connect(NullCredential.INSTANCE, new ClientInformation());
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @param credential the connection credential
     * @return future session wire
     * @throws IOException connection error
     */
    default FutureResponse<Wire> connect(@Nonnull Credential credential) throws IOException {
        return connect(credential, new ClientInformation());
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @param clientInformation the client information
     * @return future session wire
     * @throws IOException connection error
     */
    default FutureResponse<Wire> connect(@Nonnull ClientInformation clientInformation) throws IOException {
        return connect(NullCredential.INSTANCE, clientInformation);
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @param credential the connection credential
     * @param clientInformation the client information
     * @return future session wire
     * @throws IOException connection error
     */
    FutureResponse<Wire> connect(@Nonnull Credential credential, @Nonnull ClientInformation clientInformation) throws IOException;
}
