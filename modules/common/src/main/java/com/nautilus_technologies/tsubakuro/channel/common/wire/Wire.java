package com.nautilus_technologies.tsubakuro.channel.common.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.nautilus_technologies.tsubakuro.exception.CoreServiceException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * An abstract interface of communication path to the server.
 * This encapsulates how communicate to the server.
 * Application developer should not use this interface directly.
 */
@ThreadSafe
public interface Wire extends ServerResource {

    /**
     * send a message to the destination server.
     * <p>
     * The returned future will raise {@link CoreServiceException} on calling {@link FutureResponse#get()}.
     * This is because the server returned an erroneous response (e.g. the destination service is not found).
     * </p>
     * <p>
     * Otherwise, you can retrieve {@link Response} and its {@link Response#waitForMainResponse() main response}
     * contains reply message from the destination service specified by {@code serviceId}.
     * </p>
     * @param serviceId the destination service ID
     * @param payload the request payload
     * @return a future of the response
     * @throws IOException if I/O error was occurred while sending message
     */
    FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload) throws IOException;

    /**
     * send a message to the destination server.
     * <p>
     * The returned future will raise {@link CoreServiceException} on calling {@link FutureResponse#get()}.
     * This is because the server returned an erroneous response (e.g. the destination service is not found).
     * </p>
     * <p>
     * Otherwise, you can retrieve {@link Response} and its {@link Response#waitForMainResponse() main response}
     * contains reply message from the destination service specified by {@code serviceId}.
     * </p>
     * @param serviceId the destination service ID
     * @param payload the request payload
     * @return a future of the response
     * @throws IOException if I/O error was occurred while sending message
     */
    default FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload) throws IOException {
        Objects.requireNonNull(payload);
        return send(serviceId, ByteBuffer.wrap(payload));
    }

    /**
     * Closes this connection.
     * This method will be invoked one or more times.
     */
    @Override
    void close() throws IOException, InterruptedException;
}
