package com.nautilus_technologies.tsubakuro.impl.low.sql;

/**
 * An error during mock operations.
 */
public class MockError extends Error {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public MockError(String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public MockError(String message, Throwable cause) {
        super(message, cause);
    }
}
