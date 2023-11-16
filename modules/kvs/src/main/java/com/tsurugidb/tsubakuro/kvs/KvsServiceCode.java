package com.tsurugidb.tsubakuro.kvs;

import java.util.HashMap;

import javax.annotation.Nonnegative;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Code of KVS service diagnostics.
 */
public enum KvsServiceCode implements DiagnosticCode {

    /**
     * the target element does not exist.
     */
    NOT_FOUND(1),

    /**
     * the target element already exists.
     */
    ALREADY_EXISTS(2),

    /**
     * transaction operation is rollbacked by user.
     */
    USER_ROLLBACK(3),

    /**
     * the operation is waiting for other transaction
     */
    WAITING_FOR_OTHER_TRANSACTION(4),

    /**
     * Unknown error.
     */
    UNKNOWN(100),

    /**
     * I/O error.
     */
    IO_ERROR(102),

    /**
     * API arguments are invalid.
     */
    INVALID_ARGUMENT(103),

    /**
     * API state is invalid.
     */
    INVALID_STATE(104),

    /**
     * operation is unsupported.
     */
    UNSUPPORTED(105),

    /**
     * transaction operation met an user-defined error.
     * <p>
     * this code is returned only from transaction_exec() and transaction_commit()
     * </p>
     */
    USER_ERROR(106),

    /**
     * transaction is aborted
     */
    ABORTED(107),

    /**
     * transaction is aborted, but retry might resolve the situation
     */
    ABORT_RETRYABLE(108),

    /**
     * api call timed out
     */
    TIME_OUT(109),

    /**
     * the feature is not yet implemented
     */
    NOT_IMPLEMENTED(110),

    /**
     * the operation is not valid
     */
    ILLEGAL_OPERATION(111),

    /**
     * the operation conflicted on write preserve
     */
    CONFLICT_ON_WRITE_PRESERVE(112),

    /**
     * long tx issued write operation without preservation
     */
    WRITE_WITHOUT_WRITE_PRESERVE(114),

    /**
     * transaction is inactive
     * <p>
     * transaction is inactive since it's already committed or aborted. The request is failed.
     * </p>
     */
    INACTIVE_TRANSACTION(115),

    /**
     * requested operation is blocked by concurrent operation
     * <p>
     * the request cannot be fulfilled due to the operation concurrently executed by other transaction.
     * After the blocking transaction completes, re-trying the request may lead to different result.
     * </p>
     */
    BLOCKED_BY_CONCURRENT_OPERATION(116),

    /**
     * reached resource limit and request could not be accomplished
     */
    RESOURCE_LIMIT_REACHED(117),

    /**
     * key length passed to the API is invalid
     */
    INVALID_KEY_LENGTH(118),

    /**
     * The operation result data is too large.
     */
    RESULT_TOO_LARGE(1_001),

    /**
     * Target resource is not authorized.
     */
    NOT_AUTHORIZED(2_001),


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

    private static final HashMap<Integer, KvsServiceCode> MAP = new HashMap<>();

    static {
        for (var v : KvsServiceCode.values()) {
            MAP.put(v.getCodeNumber(), v);
        }
    }

    /**
     * get KvsServiceCode object with code
     * @param code the code of the KvsServiceCode
     * @return KvsServiceCode with the valid code,
     *          or KvsServiceCode.UNKNOWN if the code is unknown.
     */
    public static KvsServiceCode getInstance(int code) {
        KvsServiceCode v = MAP.get(code);
        if (v != null) {
            return v;
        } else {
            return KvsServiceCode.UNKNOWN;
        }
    }

}
