package com.tsurugidb.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
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
     * Retrieves sub-responses in this response.
     * You can read each sub-responses data only once.
     * Even if If sub-responses data have not been completed, you can retrieve them partially.
     * The stream will be blocked when the stream position reached to the incomplete area of the sub-response.
     * @param id the sub-responses name
     * @return contents of body of the sub-response
     * @throws NoSuchElementException if there is no such the data channel
     * @throws IOException if I/O error was occurred while opening the sub-responses
     * @throws ServerException if server error was occurred while opening the sub-responses
     * @throws InterruptedException if interrupted by other threads while opening the sub-responses
     */
    default InputStream openSubResponse(String id) throws NoSuchElementException, IOException, ServerException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves sub-responses in this response.
     * You can read each sub-responses data only once.
     * Even if If sub-responses data have not been completed, you can retrieve them partially.
     * The stream will be blocked when the stream position reached to the incomplete area of the sub-response.
     * @param id the sub-responses name
     * @param timeout the maximum time to wait
     * @param unit the time unit of {@code timeout}
     * @return contents of body of the sub-response
     * @throws NoSuchElementException if there is no such the data channel
     * @throws IOException if I/O error was occurred while opening the sub-responses
     * @throws ServerException if server error was occurred while opening the sub-responses
     * @throws InterruptedException if interrupted by other threads while opening the sub-responses
     * @throws TimeoutException if the wait time out;
     */
    default InputStream openSubResponse(String id, long timeout, TimeUnit unit)
            throws NoSuchElementException, IOException, ServerException, InterruptedException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    /**
     * Cancel the processing request to receive the result in this response.
     * @throws IOException error occurred while sending the cancel request
     */
    default void cancel() throws IOException {
        throw new UnsupportedOperationException();
    }
}
