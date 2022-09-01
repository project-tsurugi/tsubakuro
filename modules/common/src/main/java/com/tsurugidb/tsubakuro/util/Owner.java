package com.tsurugidb.tsubakuro.util;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Represents an ownership of {@link ServerResource}.
 * @param <T> the value type
 */
public class Owner<T extends ServerResource> implements ServerResource {

    private final AtomicReference<T> ownership;

    /**
     * Creates a new object.
     * @param object the object to hold
     */
    public Owner(T object) {
        this.ownership = new AtomicReference<>(object);
    }

    /**
     * Creates a new object.
     * @param <T> the value type
     * @param object the object to hold
     * @return the created object
     */
    public static <T extends ServerResource> Owner<T> of(T object) {
        return new Owner<>(object);
    }

    /**
     * Returns the holding object.
     * @return the holding value, or {@code null} if this has no objects.
     */
    public T get() {
        return ownership.get();
    }

    /**
     * Releases the holding object.
     * After call this, the holding object becomes absent.
     * @return the holding value, or {@code null} if this has no objects.
     * @see #get()
     */
    public T release() {
        return ownership.getAndSet(null);
    }

    /**
     * Returns a new ownership of the holding object.
     * After call this, the holding object becomes absent.
     * @return a new ownership
     */
    public Owner<T> move() {
        return Owner.of(release());
    }

    @Override
    public void setCloseTimeout(@Nonnull Timeout timeout) {
        Objects.requireNonNull(timeout);
        var resource = ownership.get();
        if (Objects.nonNull(resource)) {
            resource.setCloseTimeout(timeout);
        }
    }

    /**
     * Closes the holding object only if it is present.
     */
    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        close(ownership.getAndSet(null));
    }

    @Override
    public String toString() {
        return Objects.toString(ownership);
    }

    /**
     * Closes the given server resource.
     * @param resource the target resource
     * @throws IOException if I/O error occurred while disposing server resource
     * @throws ServerException if server error occurred while disposing server resource
     * @throws InterruptedException if interrupted while disposing server resource
     */
    public static void close(@Nullable ServerResource resource)
            throws IOException, ServerException, InterruptedException {
        if (Objects.nonNull(resource)) {
            resource.close();
        }
    }
}
