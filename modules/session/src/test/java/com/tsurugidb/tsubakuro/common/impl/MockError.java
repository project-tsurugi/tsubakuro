package com.tsurugidb.tsubakuro.common.impl;

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
