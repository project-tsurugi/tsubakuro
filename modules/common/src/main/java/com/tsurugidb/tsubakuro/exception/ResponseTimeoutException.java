package com.tsurugidb.tsubakuro.exception;

import java.io.IOException;

/**
 * AN exception occurred at waiting response is timeout.
 */
public class ResponseTimeoutException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public ResponseTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     */
    public ResponseTimeoutException() {
        this(null, null);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public ResponseTimeoutException(String message) {
        this(message, null);
    }

    /**
     * Creates a new instance.
     * @param cause the original cause
     */
    public ResponseTimeoutException(Throwable cause) {
        this(null, cause);
    }

}
