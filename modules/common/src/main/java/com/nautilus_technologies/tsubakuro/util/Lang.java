package com.nautilus_technologies.tsubakuro.util;

/**
 * Java language utilities.
 */
public final class Lang {

    private Lang() {
        throw new AssertionError();
    }

    /**
     * Does nothing.
     */
    public static void pass() {
        return;
    }

    /**
     * Does nothing.
     * @param <T> the return type
     * @param value the value
     * @return the value
     */
    public static <T> T pass(T value) {
        return value;
    }
}
