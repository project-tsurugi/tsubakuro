package com.tsurugidb.tsubakuro.debug;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.util.Doc;

/**
 * Code of debugging service diagnostics.
 */
@Doc(
        value = "Debugging service is designed to debug the database itself.",
        note = "Please consider disabling this service for regular database use.")
public enum DebugServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    @Doc("unknown error was occurred in the debugging service.")
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
