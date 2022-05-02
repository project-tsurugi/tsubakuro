package com.nautilus_technologies.tsubakuro.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * Represents a resource which is on the server or corresponding to it.
 * You must {@link #close()} after use this object.
 */
public interface ServerResource extends AutoCloseable {

    /**
     * Sets timeout of {@link #close()} operation.
     * If this object does not take long time, this operation does nothing.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    default void setCloseTimeout(long timeout, TimeUnit unit) {
        Lang.pass();
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
