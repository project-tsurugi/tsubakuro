package com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

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
        Objects.nonNull(endpoint);
        return ConnectorHelper.create(URI.create(endpoint));
    }

    /**
     * Creates a new connector for the end-point URI.
     * @param endpoint the end-point URI
     * @return the corresponded connector
     * @throws NoSuchElementException if there is no suitable connector implementation for the URI
     */
    static Connector create(@Nonnull URI endpoint) {
        Objects.nonNull(endpoint);
        return ConnectorHelper.create(endpoint);
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @return future session wire
     * @throws IOException connection error
     */
    default FutureResponse<SessionWire> connect() throws IOException {
        return connect(NullCredential.INSTANCE);
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @param credential the connection credential
     * @return future session wire
     * @throws IOException connection error
     */
    FutureResponse<SessionWire> connect(@Nonnull Credential credential) throws IOException;
}
