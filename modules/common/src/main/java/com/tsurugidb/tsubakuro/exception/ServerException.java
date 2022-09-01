package com.tsurugidb.tsubakuro.exception;

/**
 * An exception which occurs if individual Tsurugi services return an erroneous response.
 */
public abstract class ServerException extends Exception implements DiagnosticCode.Provider {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public ServerException() {
        super();
    }

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public ServerException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public ServerException(String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     * @param cause the original cause
     */
    public ServerException(Throwable cause) {
        super(cause);
    }
}
