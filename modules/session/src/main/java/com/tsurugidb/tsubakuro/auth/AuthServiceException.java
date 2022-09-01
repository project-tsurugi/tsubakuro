package com.tsurugidb.tsubakuro.auth;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * An exception which occurs on auth service.
 */
public class AuthServiceException extends ServerException {

    private static final long serialVersionUID = 1L;

    private final AuthServiceCode code;

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     * @param cause the original cause
     */
    public AuthServiceException(@Nonnull AuthServiceCode code, @Nullable String message, @Nullable Throwable cause) {
        super(buildMessage(code, message), cause);
        this.code = code;
    }

    private static String buildMessage(AuthServiceCode code, @Nullable String message) {
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
    public AuthServiceException(@Nonnull AuthServiceCode code) {
        this(code, null, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     */
    public AuthServiceException(@Nonnull AuthServiceCode code, @Nullable String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param cause the original cause
     */
    public AuthServiceException(@Nonnull AuthServiceCode code, @Nullable Throwable cause) {
        this(code, null, cause);
    }

    @Override
    public AuthServiceCode getDiagnosticCode() {
        return code;
    }
}
