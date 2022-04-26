package com.nautilus_technologies.tsubakuro.low.backup;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;

/**
 * Code of datastore service diagnostics.
 */
public enum DatastoreServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    UNKNOWN(0),

    ;
    private final int codeNumber;

    DatastoreServiceCode(int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`DSS-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "DSS"; //$NON-NLS-1$

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