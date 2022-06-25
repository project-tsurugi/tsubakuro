package com.nautilus_technologies.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * Represents unparsed response from the Tsurugi OLTP server.
 */
public interface Response extends ServerResource {

    /**
     * Returns whether or not the main response body is already available.
     * That is, {@link #waitForMainResponse()} returns the main response data without blocking.
     * Behavior is undefined if this response is already {@link Response#close() closed}.
     * @return {@code true} if the main response is already available, otherwise {@code false}
     */
    default boolean isMainResponseReady() {
        return false;
    }

    /**
     * Returns the main response body.
     * If the main response body is not ready, this operation was blocked until it would be ready.
     * @return ByteBuffer of the main response body
     * @throws IOException if I/O error was occurred while retrieving main response body
     * @throws ServerException if server error was occurred while retrieving main response body
     * @throws InterruptedException if interrupted while retrieving main response body
     */
    ByteBuffer waitForMainResponse() throws IOException, ServerException, InterruptedException;

    /**
     * Returns the main response body.
     * If the main response body is not ready, this operation was blocked until it would be ready.
     * @param timeout the maximum time to wait
     * @param unit the time unit of {@code timeout}
     * @return ByteBuffer of the main response body
     * @throws IOException if I/O error was occurred while retrieving main response body
     * @throws ServerException if server error was occurred while retrieving main response body
     * @throws InterruptedException if interrupted while retrieving main response body
     * @throws TimeoutException if the wait time out;
     *      please attention that this exception may occur shorter time than the {@code timeout}
     */
    ByteBuffer waitForMainResponse(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException;

    /**
     * Clone the Response without using super.clone(). It is usesd when QueryMode has been set.
     * @return Response  to receive the second response forwarded from the server when using query mode.
     */
    Response duplicate();

    /**
     * Set this response to involve data transfer using resultSet.
     */
    void setResultSetMode();

    void release();
}
