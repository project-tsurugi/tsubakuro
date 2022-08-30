package com.nautilus_technologies.tsubakuro.exception;

import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tateyama.proto.SqlStatus;

/**
 * Code of server core diagnostics.
 */
public enum SqlServiceCode implements DiagnosticCode {

    /**
     * Ok, not error, and thus it should not be used.
     */
    OK(0, SqlStatus.Status.OK),

    /**
     * Not Found
     */
    NOT_FOUND(1, SqlStatus.Status.NOT_FOUND),

    /**
     * Already Exists
     */
    ALREADY_EXISTS(2, SqlStatus.Status.ALREADY_EXISTS),

    /**
     * system is something wrong.
     */
    USER_ROLLBACK(3, SqlStatus.Status.USER_ROLLBACK),

    /**
     * operation is not supported.
     */
    ERR_UNKNOWN(-1, SqlStatus.Status.ERR_UNKNOWN),
    
    /**
     * ERR_IO_ERROR
     */
    ERR_IO_ERROR(-2, SqlStatus.Status.ERR_IO_ERROR),

    /**
     * ERR_PARSE_ERROR
     */
    ERR_PARSE_ERROR(-3, SqlStatus.Status.ERR_PARSE_ERROR),

    /**
     * ERR_TRANSLATOR_ERROR
     */
    ERR_TRANSLATOR_ERROR(-4, SqlStatus.Status.ERR_TRANSLATOR_ERROR),

    /**
     * ERR_COMPILER_ERROR
     */
    ERR_COMPILER_ERROR(-5, SqlStatus.Status.ERR_COMPILER_ERROR),

    /**
     * ERR_INVALID_ARGUMENT
     */
    ERR_INVALID_ARGUMENT(-6, SqlStatus.Status.ERR_INVALID_ARGUMENT),

    /**
     * ERR_INVALID_STATE
     */
    ERR_INVALID_STATE(-7, SqlStatus.Status.ERR_INVALID_STATE),

    /**
     * ERR_UNSUPPORTED
     */
    ERR_UNSUPPORTED(-8, SqlStatus.Status.ERR_UNSUPPORTED),

    /**
     * ERR_USER_ERROR
     */
    ERR_USER_ERROR(-9, SqlStatus.Status.ERR_USER_ERROR),

    /**
     * ERR_ABORTED
     */
    ERR_ABORTED(-10, SqlStatus.Status.ERR_ABORTED),

    /**
     * ERR_ABORTED_RETRYABLE
     */
    ERR_ABORTED_RETRYABLE(-11, SqlStatus.Status.ERR_ABORTED_RETRYABLE),

    /**
     * ERR_NOT_FOUND
     */
    ERR_NOT_FOUND(-12, SqlStatus.Status.ERR_NOT_FOUND),

    /**
     * ERR_ALREADY_EXISTS
     */
    ERR_ALREADY_EXISTS(-13, SqlStatus.Status.ERR_ALREADY_EXISTS),

    /**
     * ERR_INCONSISTENT_INDEX
     */
    ERR_INCONSISTENT_INDEX(-14, SqlStatus.Status.ERR_INCONSISTENT_INDEX),

    /**
     * ERR_TIME_OUT
     */
    ERR_TIME_OUT(-15, SqlStatus.Status.ERR_TIME_OUT),

    /**
     * ERR_INTEGRITY_CONSTRAINT_VIOLATION
     */
    ERR_INTEGRITY_CONSTRAINT_VIOLATION(-16, SqlStatus.Status.ERR_INTEGRITY_CONSTRAINT_VIOLATION),

    /**
     * ERR_EXPRESSION_EVALUATION_FAILURE
     */
    ERR_EXPRESSION_EVALUATION_FAILURE(-17, SqlStatus.Status.ERR_EXPRESSION_EVALUATION_FAILURE),

    /**
     * ERR_UNRESOLVED_HOST_VARIABLE
     */
    ERR_UNRESOLVED_HOST_VARIABLE(-18, SqlStatus.Status.ERR_UNRESOLVED_HOST_VARIABLE),

    /**
     * ERR_TYPE_MISMATCH
     */
    ERR_TYPE_MISMATCH(-19, SqlStatus.Status.ERR_TYPE_MISMATCH),

    /**
     * ERR_NOT_IMPLEMENTED
     */
    ERR_NOT_IMPLEMENTED(-20, SqlStatus.Status.ERR_NOT_IMPLEMENTED),

    /**
     * ERR_ILLEGAL_OPERATION
     */
    ERR_ILLEGAL_OPERATION(-21, SqlStatus.Status.ERR_ILLEGAL_OPERATION),

    /**
     * ERR_MISSING_OPERATION_TARGET
     */
    ERR_MISSING_OPERATION_TARGET(-22, SqlStatus.Status.ERR_MISSING_OPERATION_TARGET),

    /**
     * ERR_CONFLICT_ON_WRITE_PRESERVE
     */
    ERR_CONFLICT_ON_WRITE_PRESERVE(-23, SqlStatus.Status.ERR_CONFLICT_ON_WRITE_PRESERVE),

    ;
    private final int codeNumber;

    private final SqlStatus.Status mapping;

    SqlServiceCode(int codeNumber, SqlStatus.Status mapping) {
        this.codeNumber = codeNumber;
        this.mapping = mapping;
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

    private static final EnumMap<SqlStatus.Status, SqlServiceCode> PROTO_MAPPING;
    static {
        Logger logger = LoggerFactory.getLogger(SqlServiceCode.class);
        PROTO_MAPPING = new EnumMap<>(SqlStatus.Status.class);
        for (var code : values()) {
            if (PROTO_MAPPING.putIfAbsent(code.mapping, code) != null) {
                logger.warn("conflict code mapping: {}.{}", //$NON-NLS-1$
                        SqlStatus.Status.class.getName(),
                        code.mapping.name());
            }
        }
        for (var proto : SqlStatus.Status.values()) {
            if (!PROTO_MAPPING.containsKey(proto)) {
                logger.warn("unknown code mapping: {}.{}", //$NON-NLS-1$
                        SqlStatus.Status.class.getName(),
                        proto.name());
            }
        }
    }

    /**
     * Returns the corresponded diagnostic code.
     * @param code the original code
     * @return the corresponded diagnostic code, or {@link #UNKNOWN} if there is no suitable mapping
     */
    public static SqlServiceCode valueOf(SqlStatus.Status code) {
        return PROTO_MAPPING.getOrDefault(code, ERR_UNKNOWN);
    }
}
