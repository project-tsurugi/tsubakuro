package com.tsurugidb.tsubakuro.exception;

/**
 * An exception which occurs if server returns a broken response message..
 */
public class BrokenResponseException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public BrokenResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     */
    public BrokenResponseException() {
        this(null, null);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public BrokenResponseException(String message) {
        this(message, null);
    }

    /**
     * Creates a new instance.
     * @param cause the original cause
     */
    public BrokenResponseException(Throwable cause) {
        this(null, cause);
    }
}
