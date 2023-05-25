package com.tsurugidb.tsubakuro.kvs;

import javax.annotation.Nonnegative;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Code of KVS service diagnostics.
 */
public enum KvsServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    UNKNOWN(0),

    /**
     * The operation is not implemented yet.
     */
    NOT_IMPLEMENTED(1),

    /**
     * The operation result data is too large.
     */
    RESULT_TOO_LARGE(1_001),

    /**
     * The operation exceeded the resource limit.
     */
    RESOURCE_LIMIT_REACHED(1_002),

    /**
     * Target resource is not authorized.
     */
    NOT_AUTHORIZED(2_001),

    /**
     * Transaction is not active.
     */
    TRANSACTION_INACTIVE(10_001),

    /**
     * Transaction is aborted by the operation.
     */
    TRANSACTION_ABORTED(11_001),

    /**
     * Transaction is aborted by the operation (but may success by retrying the sequence of operations) .
     */
    TRANSACTION_ABORTED_RETRYABLE(11_002),

    /**
     * Transaction is aborted by conflict to other write preservation.
     */
    CONFLICT_ON_WRITE_PRESERVE(12_001),

    /**
     * Transaction is aborted by writing out of write preservation, or writing in read only transaction.
     */
    WRITE_PROTECTED(12_002),

    /**
     * The specified table is not found.
     */
    TABLE_NOT_FOUND(20_001),

    /**
     * The specified column is not found.
     */
    COLUMN_NOT_FOUND(20_002),

    /**
     * The column type is inconsistent.
     */
    COLUMN_TYPE_MISMATCH(20_003),

    /**
     * The search key is mismatch for the table or index.
     */
    MISMATCH_KEY(20_011),

    /**
     * Several columns are not specified in {@code PUT} operation.
     */
    INCOMPLETE_COLUMNS(20_021),

    /**
     * Operations was failed by integrity constraint violation.
     */
    INTEGRITY_CONSTRAINT_VIOLATION(30_001),

    ;
    private final int codeNumber;

    KvsServiceCode(@Nonnegative int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`KVS-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "KVS"; //$NON-NLS-1$

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
