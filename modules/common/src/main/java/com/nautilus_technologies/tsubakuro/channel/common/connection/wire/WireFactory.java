package com.nautilus_technologies.tsubakuro.channel.common.connection.wire;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Credential;
import com.nautilus_technologies.tsubakuro.exception.ConnectionException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * Provides {@link Wire} object for individual URIs.
 */
@ThreadSafe
public interface WireFactory {

    /**
     * Returns whether or not this recognizes the given end-point URI.
     * @param endpoint the target end-point URI
     * @return {@code true} if this recognizes the given end-point URI,
     *      otherwise {@code false}
     */
    boolean accepts(URI endpoint);

    /**
     * Establishes a new connection to the server using its end-point URI.
     * @param endpoint the target end-point URI
     * @param credential credential information to connect to the server
     * @return future of a wire object which represents a connection to the target server
     * @throws ConnectionException if connection was failed
     * @see #accepts(URI)
     */
    FutureResponse<? extends Wire> create(URI endpoint, Credential credential) throws ConnectionException;
}
