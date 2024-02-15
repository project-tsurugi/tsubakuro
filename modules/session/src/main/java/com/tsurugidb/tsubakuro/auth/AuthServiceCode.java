package com.tsurugidb.tsubakuro.auth;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.util.Doc;

/**
 * Code of auth service diagnostics.
 */
@Doc(
        value = "Authentication service is designed for external use of Tsurugi's authentication mechanism.",
        note = "This does not work correct because authentication is not yet available in Tsurugi.")
public enum AuthServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    @Doc("unknown error was occurred in the authentication service.")
    UNKNOWN(0),

    /**
     * Authentication information is not found.
     */
    @Doc(
            value = "authentication information is not found in this session.",
            note = "Credentials may be not specified when establishing a session.")
    NOT_AUTHENTICATED(1_01),

    ;
    private final int codeNumber;

    AuthServiceCode(int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`AUT-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "AUT"; //$NON-NLS-1$

    @Override
    public String getStructuredCode() {
        return String.format("%s-%05d", PREFIX_STRUCTURED_CODE, getCodeNumber()); //$NON-NLS-1$
    }

    @Override
    public int getCodeNumber() {
        return codeNumber;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", getStructuredCode(), name()); //$NON-NLS-1$
    }
}
