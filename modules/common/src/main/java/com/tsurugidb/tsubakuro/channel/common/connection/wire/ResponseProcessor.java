package com.tsurugidb.tsubakuro.channel.common.connection.wire;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * Processes the {@link Response} and returns request specific result value.
 * @param <T> the result value type
 */
public interface ResponseProcessor<T> {

    /**
     * Returns whether or not this requires {@link Response#waitForMainResponse()} is already available
     * before {@link #process(Response)} is invoked.
     * That is, if this returns {@code true},
     * {@link java.util.concurrent.Future#isDone() response.getPayload().isDone()} is always true.
     * @return {@code true} if payload is available
     */
    default boolean isMainResponseRequired() {
        return true;
    }

    /**
     * Processes the {@link Response} object and returns a request specific result value.
     * If the result value is not corresponding to the server resource,
     * you must dispose by using {@link Response#close()} before complete this method.
     * @param response the process target
     * @return the processed result
     * @throws IOException if I/O error was occurred while processing the response
     * @throws ServerException if server error was occurred while processing the response
     * @throws InterruptedException if interrupted while processing the response
     */
    T process(@Nonnull Response response) throws IOException, ServerException, InterruptedException;

    /**
     * Processes the {@link Response} object and returns a request specific result value.
     * If the result value is not corresponding to the server resource,
     * you must dispose by using {@link Response#close()} before complete this method.
     * @param response the process target
     * @param timeout the maximum time to wait the subresponse
     * @return the processed result
     * @throws IOException if I/O error was occurred while processing the response
     * @throws ServerException if server error was occurred while processing the response
     * @throws InterruptedException if interrupted while processing the response
     */
    default T process(@Nonnull Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
        return process(response);
    }
}
