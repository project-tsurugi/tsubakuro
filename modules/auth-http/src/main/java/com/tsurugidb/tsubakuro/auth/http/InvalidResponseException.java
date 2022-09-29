package com.tsurugidb.tsubakuro.auth.http;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * Represents an exception which occurs if auth service response was something wrong.
 */
public class InvalidResponseException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public InvalidResponseException(@Nullable String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public InvalidResponseException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
