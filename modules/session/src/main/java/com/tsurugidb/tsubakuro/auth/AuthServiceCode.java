package com.tsurugidb.tsubakuro.auth;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Code of auth service diagnostics.
 */
public enum AuthServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    UNKNOWN(0),

    /**
     * Authentication information is not found.
     */
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
