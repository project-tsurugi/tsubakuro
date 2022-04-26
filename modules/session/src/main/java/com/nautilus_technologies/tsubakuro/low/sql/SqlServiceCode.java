package com.nautilus_technologies.tsubakuro.low.sql;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;

/**
 * Code of SQL service diagnostics.
 */
public enum SqlServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    UNKNOWN(0),

    ;
    private final int codeNumber;

    SqlServiceCode(int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`SQL-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "SQL"; //$NON-NLS-1$

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
