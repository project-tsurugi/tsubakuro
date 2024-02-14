package com.tsurugidb.tsubakuro.exception;

import com.tsurugidb.tsubakuro.util.Doc;

/**
 * A test mock of {@link DiagnosticCode}.
 */
public enum MockDiagnosticCode implements DiagnosticCode {

    /**
     * example constant.
     */
    @Doc("OK")
    TESTING,

    ;

    @Override
    public String getStructuredCode() {
        return String.format("MCK-%05d", getCodeNumber());
    }

    @Override
    public int getCodeNumber() {
        return 0;
    }
}
