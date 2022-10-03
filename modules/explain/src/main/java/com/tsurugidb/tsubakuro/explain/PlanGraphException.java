package com.tsurugidb.tsubakuro.explain;

import javax.annotation.Nullable;

/**
 * Exception occurred when execution plan information is not valid.
 */
public class PlanGraphException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public PlanGraphException() {
        super();
    }

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public PlanGraphException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public PlanGraphException(@Nullable String message) {
        super(message);
    }
}
