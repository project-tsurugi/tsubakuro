package com.nautilus_technologies.tsubakuro.low.common;

import java.io.IOException;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import  com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import  com.nautilus_technologies.tsubakuro.channel.common.connection.Credential;
import  com.nautilus_technologies.tsubakuro.channel.common.connection.NullCredential;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;

/**
 * Builds a {@link Session} object.
 * @see #connect(String)
 * @see #create()
 */
public final class SessionBuilder {

    private final Connector connector;

    private Credential connectionCredential = NullCredential.INSTANCE;

    private SessionBuilder(Connector connector) {
        assert connector != null;
        this.connector = connector;
    }

    /**
     * Starts building a new {@link Session}.
     * @param endpoint the end-point URI string
     * @return a new {@link SessionBuilder} to connect to the end-point
     * @throws IllegalArgumentException if the end-point string is not a valid URI
     * @throws NoSuchElementException if there is no suitable connector implementation for the URI
     */
    public static SessionBuilder connect(@Nonnull String endpoint) {
        Objects.requireNonNull(endpoint);
        return connect(Connector.create(endpoint));
    }

    /**
     * Starts building a new {@link Session}.
     * @param endpoint the end-point URI
     * @return a new {@link SessionBuilder} to connect to the end-point
     * @throws NoSuchElementException if there is no suitable connector implementation for the URI
     */
    public static SessionBuilder connect(@Nonnull URI endpoint) {
        return connect(Connector.create(endpoint));
    }

    /**
     * Starts building a new {@link Session}.
     * @param connector the target connector
     * @return a new {@link SessionBuilder} to connect using the connector
     */
    public static SessionBuilder connect(@Nonnull Connector connector) {
        Objects.requireNonNull(connector);
        return new SessionBuilder(connector);
    }

    /**
     * Sets credential information to connect.
     * @param credential the credential information
     * @return this
     */
    public SessionBuilder withCredential(@Nonnull Credential credential) {
        Objects.requireNonNull(credential);
        this.connectionCredential = credential;
        return this;
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * This operation will block until the connection was established,
     * please consider to use {@link #create(long, TimeUnit)}.
     * @return the established connection session
     * @throws IOException if I/O error was occurred during connection
     * @throws ServerException if server error was occurred during connection
     * @throws InterruptedException if interrupted while
     * @see #create(long, TimeUnit)
     */
    public Session create() throws IOException, ServerException, InterruptedException {
        try (var fWire = connector.connect(connectionCredential)) {
            return create0(fWire.get());
        }
    }

    /**
     * Establishes a connection to the Tsurugi server.
     * @param timeout the maximum time to wait
     * @param unit the time unit of {@code timeout}
     * @return the established connection session
     * @throws IOException if I/O error was occurred during connection
     * @throws ServerException if server error was occurred during connection
     * @throws InterruptedException if interrupted while
     * @throws TimeoutException if the wait time out
     */
    public Session create(long timeout, @Nonnull TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        Objects.requireNonNull(unit);
        try (var fWire = connector.connect(connectionCredential)) {
            return create0(fWire.get(timeout, unit));
        }
    }

    private static Session create0(Wire wire) throws IOException, ServerException, InterruptedException {
        assert wire != null;
        var session = new SessionImpl();
        boolean green = false;
        try {
            session.connect(wire);
            green = true;
            return session;
        } finally {
            if (!green) {
                session.close();
                wire.close();
            }
        }
    }
}
