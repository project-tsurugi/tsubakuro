package com.tsurugidb.tsubakuro.common;

import java.io.IOException;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.diagnostic.JMXAgent;
import com.tsurugidb.tsubakuro.diagnostic.common.SessionInfo;

/**
 * Builds a {@link Session} object.
 * @see #connect(String)
 * @see #create()
 */
public final class SessionBuilder {

    private final Connector connector;

    private Credential connectionCredential = NullCredential.INSTANCE;

    private SessionInfo sessionInfo;

    private SessionBuilder(Connector connector) {
        assert connector != null;
        this.connector = connector;
        this.sessionInfo = JMXAgent.sessionInfo();
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
            return  new SessionImpl(fWire.get());
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
            var sessionImpl = new SessionImpl(fWire.get(timeout, unit));
            sessionInfo.addSession(sessionImpl);
            return sessionImpl;
        }
    }
}
