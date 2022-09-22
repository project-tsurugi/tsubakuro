package com.tsurugidb.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;

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
     * Returns whether or not the sub response body is already available.
     * That is, {@link #waitForSecondResponse()} returns the sub response data without blocking.
     * Behavior is undefined if this response is already {@link Response#close() closed}.
     * @return {@code true} if the sub response is already available, otherwise {@code false}
     */
    default boolean isSecondResponseReady() {
        return false;
    }

    /**
     * Returns the sub response body.
     * If the sub response body is not ready, this operation was blocked until it would be ready.
     * @return ByteBuffer of the sub response body
     * @throws IOException if I/O error was occurred while retrieving sub response body
     * @throws ServerException if server error was occurred while retrieving sub response body
     * @throws InterruptedException if interrupted while retrieving sub response body
     */
    default ByteBuffer waitForSecondResponse() throws IOException, ServerException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the sub response body.
     * If the sub response body is not ready, this operation was blocked until it would be ready.
     * @param timeout the maximum time to wait
     * @param unit the time unit of {@code timeout}
     * @return ByteBuffer of the sub response body
     * @throws IOException if I/O error was occurred while retrieving sub response body
     * @throws ServerException if server error was occurred while retrieving sub response body
     * @throws InterruptedException if interrupted while retrieving sub response body
     * @throws TimeoutException if the wait time out;
     *      please attention that this exception may occur shorter time than the {@code timeout}
     */
    default ByteBuffer waitForSecondResponse(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        throw new UnsupportedOperationException();
    }
}
