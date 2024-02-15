package com.tsurugidb.tsubakuro.sql.exception;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * write operation was requested using RTX
 */
public class WriteOperationByRtxException extends RestrictedOperationException {

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     * @param cause the original cause
     */
    public WriteOperationByRtxException(@Nonnull SqlServiceCode code, @Nullable String message, @Nullable Throwable cause) {
        super(code, message, cause);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     */
    public WriteOperationByRtxException(@Nonnull SqlServiceCode code) {
        this(code, null, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     */
    public WriteOperationByRtxException(@Nonnull SqlServiceCode code, @Nullable String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param cause the original cause
     */
    public WriteOperationByRtxException(@Nonnull SqlServiceCode code, @Nullable Throwable cause) {
        this(code, null, cause);
    }
}
