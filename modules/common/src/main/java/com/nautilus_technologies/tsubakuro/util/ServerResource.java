package com.nautilus_technologies.tsubakuro.util;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * Represents a resource which is on the server or corresponding to it.
 * You must {@link #close()} after use this object.
 */
public interface ServerResource extends AutoCloseable {

    /**
     * Sets close timeout.
     * This is only effective if this resource considers close timeout.
     * @param timeout the timeout setting
     */
    default void setCloseTimeout(@Nonnull Timeout timeout) {
        return;
    }

    @Override
    void close() throws ServerException, IOException, InterruptedException;

    /**
     * Handles closed {@link ServerResource}.
     */
    @FunctionalInterface
    public interface CloseHandler {
        /**
         * Handles closed {@link ServerResource}.
         * @param resource the resource that {@link ServerResource#close()} was invoked
         */
        void onClosed(@Nonnull ServerResource resource);
    }
}
