package com.tsurugidb.tsubakuro.debug;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Code of debugging service diagnostics.
 */
public enum DebugServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    UNKNOWN(0),

    ;
    private final int codeNumber;

    DebugServiceCode(int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`DBG-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "DBG"; //$NON-NLS-1$

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
