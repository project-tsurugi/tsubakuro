package com.tsurugidb.tsubakuro.console.executor;

import java.io.IOException;

/**
 * Supplies an I/O object.
 * @param <T> the object type.
 */
@FunctionalInterface
public interface IoSupplier<T> {

    /**
     * Supplies an object.
     * @return the supplies object
     * @throws IOException if I/O error occurred while supplying the object
     */
    T get() throws IOException;
}
