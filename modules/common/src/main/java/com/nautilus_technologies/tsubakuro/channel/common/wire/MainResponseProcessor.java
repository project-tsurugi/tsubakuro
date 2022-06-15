package com.nautilus_technologies.tsubakuro.channel.common.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * Processes the {@link Response#waitForMainResponse() main response} and returns request specific result value.
 * @param <T> the result value type
 */
@FunctionalInterface
public interface MainResponseProcessor<T> {

    /**
     * Processes the main response and returns a request specific result value.
     * @param payload response payload
     * @return the processed result
     * @throws IOException if I/O error was occurred while processing the response
     * @throws ServerException if server error was occurred while processing the response
     * @throws InterruptedException if interrupted while processing the response
     */
    T process(@Nonnull ByteBuffer payload) throws IOException, ServerException, InterruptedException;

    /**
     * Returns {@link ResponseProcessor} view of this object.
     * @return the corresponding {@link ResponseProcessor}
     */
    default ResponseProcessor<T> asResponseProcessor() {
        var self = this;
        return new ResponseProcessor<>() {

            @Override
            public boolean isMainResponseRequired() {
                return true;
            }

            @Override
            public T process(@Nonnull Response response) throws IOException, ServerException, InterruptedException {
                Objects.requireNonNull(response);
                try (response) {
                    return self.process(response.waitForMainResponse());
                }
            }

            @Override
            public String toString() {
                return String.valueOf(self);
            }
        };
    }
}
