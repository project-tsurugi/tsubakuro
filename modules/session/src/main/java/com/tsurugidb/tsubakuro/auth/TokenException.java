package com.tsurugidb.tsubakuro.auth;

import java.io.IOException;

/**
 * An exception which occurs if auth token is something wrong.
 */
public class TokenException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public TokenException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     */
    public TokenException() {
        this(null, null);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public TokenException(String message) {
        this(message, null);
    }

    /**
     * Creates a new instance.
     * @param cause the original cause
     */
    public TokenException(Throwable cause) {
        this(null, cause);
    }
}
