package com.tsurugidb.tsubakuro.debug;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * An exception which occurs on debugging service.
 */
public class DebugServiceException extends ServerException {

    private static final long serialVersionUID = 1L;

    private final DebugServiceCode code;

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     * @param cause the original cause
     */
    public DebugServiceException(@Nonnull DebugServiceCode code, @Nullable String message, @Nullable Throwable cause) {
        super(buildMessage(code, message), cause);
        this.code = code;
    }

    private static String buildMessage(DebugServiceCode code, @Nullable String message) {
        Objects.requireNonNull(code);
        if (message == null) {
            return code.getStructuredCode();
        }
        return String.format("%s: %s", code.getStructuredCode(), message); //$NON-NLS-1$
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     */
    public DebugServiceException(@Nonnull DebugServiceCode code) {
        this(code, null, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     */
    public DebugServiceException(@Nonnull DebugServiceCode code, @Nullable String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param cause the original cause
     */
    public DebugServiceException(@Nonnull DebugServiceCode code, @Nullable Throwable cause) {
        this(code, null, cause);
    }

    @Override
    public DebugServiceCode getDiagnosticCode() {
        return code;
    }
}
