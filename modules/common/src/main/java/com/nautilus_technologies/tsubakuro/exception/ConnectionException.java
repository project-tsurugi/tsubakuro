package com.nautilus_technologies.tsubakuro.exception;

import java.io.IOException;

/**
 * An exception which occurs if Tsurugi OLTP server is something wrong.
 */
public class ConnectionException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     */
    public ConnectionException() {
        this(null, null);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public ConnectionException(String message) {
        this(message, null);
    }

    /**
     * Creates a new instance.
     * @param cause the original cause
     */
    public ConnectionException(Throwable cause) {
        this(null, cause);
    }
}
