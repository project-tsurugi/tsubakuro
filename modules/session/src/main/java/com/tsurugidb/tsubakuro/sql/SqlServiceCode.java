package com.tsurugidb.tsubakuro.sql;

import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.sql.proto.SqlError;

/**
 * Code of server core diagnostics.
 */
public enum SqlServiceCode implements DiagnosticCode {

    /**
     * SQL_SERVICE_EXCEPTION
     */
    SQL_SERVICE_EXCEPTION(1000, SqlError.Code.SQL_SERVICE_EXCEPTION),

    /**
     * SQL_EXECUTION_EXCEPTION
     */
    SQL_EXECUTION_EXCEPTION(2000, SqlError.Code.SQL_EXECUTION_EXCEPTION),

    /**
     * CONSTRAINT_VIOLATION_EXCEPTION
     */
    CONSTRAINT_VIOLATION_EXCEPTION(2001, SqlError.Code.CONSTRAINT_VIOLATION_EXCEPTION),

    /**
     * UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION
     */
    UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION(2002, SqlError.Code.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION),

    /**
     * NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION
     */
    NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION(2003, SqlError.Code.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION),

    /**
     * REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION
     */
    REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION(2004, SqlError.Code.REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION),

    /**
     * CHECK_CONSTRAINT_VIOLATION_EXCEPTIONCHECK_CONSTRAINT_VIOLATION_EXCEPTION
     */
    CHECK_CONSTRAINT_VIOLATION_EXCEPTION(2005, SqlError.Code.CHECK_CONSTRAINT_VIOLATION_EXCEPTION),

    /**
     * EVALUATION_EXCEPTION
     */
    EVALUATION_EXCEPTION(2010, SqlError.Code.EVALUATION_EXCEPTION),

    /**
     * VALUE_EVALUATION_EXCEPTION
     */
    VALUE_EVALUATION_EXCEPTION(2011, SqlError.Code.VALUE_EVALUATION_EXCEPTION),

    /**
     * SCALAR_SUBQUERY_EVALUATION_EXCEPTION
     */
    SCALAR_SUBQUERY_EVALUATION_EXCEPTION(2012, SqlError.Code.SCALAR_SUBQUERY_EVALUATION_EXCEPTION),

    /**
     * TARGET_NOT_FOUND_EXCEPTION
     */
    TARGET_NOT_FOUND_EXCEPTION(2014, SqlError.Code.TARGET_NOT_FOUND_EXCEPTION),

    /**
     * TARGET_ALREADY_EXISTS_EXCEPTION
     */
    TARGET_ALREADY_EXISTS_EXCEPTION(100, SqlError.Code.TARGET_ALREADY_EXISTS_EXCEPTION),

    /**
     * INCONSISTENT_STATEMENT_EXCEPTION
     */
    INCONSISTENT_STATEMENT_EXCEPTION(2018, SqlError.Code.INCONSISTENT_STATEMENT_EXCEPTION),

    /**
     * RESTRICTED_OPERATION_EXCEPTION
     */
    RESTRICTED_OPERATION_EXCEPTION(2020, SqlError.Code.RESTRICTED_OPERATION_EXCEPTION),

    /**
     * DEPENDENCIES_VIOLATION_EXCEPTION
     */
    DEPENDENCIES_VIOLATION_EXCEPTION(2021, SqlError.Code.DEPENDENCIES_VIOLATION_EXCEPTION),

    /**
     * WRITE_OPERATION_BY_RTX_EXCEPTION
     */
    WRITE_OPERATION_BY_RTX_EXCEPTION(2022, SqlError.Code.WRITE_OPERATION_BY_RTX_EXCEPTION),

    /**
     * LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION
     */
    LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION(2023, SqlError.Code.LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION),

    /**
     * READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION
     */
    READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION(2024, SqlError.Code.READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION),

    /**
     * INACTIVE_TRANSACTION_EXCEPTION
     */
    INACTIVE_TRANSACTION_EXCEPTION(2025, SqlError.Code.INACTIVE_TRANSACTION_EXCEPTION),

    /**
     * PARAMETER_EXCEPTION
     */
    PARAMETER_EXCEPTION(2027, SqlError.Code.PARAMETER_EXCEPTION),

    /**
     * UNRESOLVED_PLACEHOLDER_EXCEPTION
     */
    UNRESOLVED_PLACEHOLDER_EXCEPTION(2028, SqlError.Code.UNRESOLVED_PLACEHOLDER_EXCEPTION),

    /**
     * LOAD_FILE_EXCEPTION
     */
    LOAD_FILE_EXCEPTION(2030, SqlError.Code.LOAD_FILE_EXCEPTION),

    /**
     * LOAD_FILE_NOT_FOUND_EXCEPTION
     */
    LOAD_FILE_NOT_FOUND_EXCEPTION(2031, SqlError.Code.LOAD_FILE_NOT_FOUND_EXCEPTION),

    /**
     * LOAD_FILE_FORMAT_EXCEPTION
     */
    LOAD_FILE_FORMAT_EXCEPTION(2032, SqlError.Code.LOAD_FILE_FORMAT_EXCEPTION),

    /**
     * DUMP_FILE_EXCEPTION
     */
    DUMP_FILE_EXCEPTION(2033, SqlError.Code.DUMP_FILE_EXCEPTION),

    /**
     * DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION
     */
    DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION(2034, SqlError.Code.DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION),

    /**
     * SQL_LIMIT_REACHED_EXCEPTION
     */
    SQL_LIMIT_REACHED_EXCEPTION(2036, SqlError.Code.SQL_LIMIT_REACHED_EXCEPTION),

    /**
     * TRANSACTION_EXCEEDED_LIMIT_EXCEPTION
     */
    TRANSACTION_EXCEEDED_LIMIT_EXCEPTION(2037, SqlError.Code.TRANSACTION_EXCEEDED_LIMIT_EXCEPTION),

    /**
     * SQL_REQUEST_TIMEOUT_EXCEPTION
     */
    SQL_REQUEST_TIMEOUT_EXCEPTION(2039, SqlError.Code.SQL_REQUEST_TIMEOUT_EXCEPTION),

    /**
     * DATA_CORRUPTION_EXCEPTION
     */
    DATA_CORRUPTION_EXCEPTION(2041, SqlError.Code.DATA_CORRUPTION_EXCEPTION),

    /**
     * SECONDARY_INDEX_CORRUPTION_EXCEPTION
     */
    SECONDARY_INDEX_CORRUPTION_EXCEPTION(2042, SqlError.Code.SECONDARY_INDEX_CORRUPTION_EXCEPTION),

    /**
     * REQUEST_FAILURE_EXCEPTION
     */
    REQUEST_FAILURE_EXCEPTION(2044, SqlError.Code.REQUEST_FAILURE_EXCEPTION),

    /**
     * TRANSACTION_NOT_FOUND_EXCEPTION
     */
    TRANSACTION_NOT_FOUND_EXCEPTION(2045, SqlError.Code.TRANSACTION_NOT_FOUND_EXCEPTION),

    /**
     * STATEMENT_NOT_FOUND_EXCEPTION
     */
    STATEMENT_NOT_FOUND_EXCEPTION(2046, SqlError.Code.STATEMENT_NOT_FOUND_EXCEPTION),

    /**
     * INTERNAL_EXCEPTION
     */
    INTERNAL_EXCEPTION(2048, SqlError.Code.INTERNAL_EXCEPTION),

    /**
     * UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION
     */
    UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION(2050, SqlError.Code.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION),

    /**
     * BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION
     */
    BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION(2052, SqlError.Code.BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION),

    /**
     * COMPILE_EXCEPTION
     */
    COMPILE_EXCEPTION(3000, SqlError.Code.COMPILE_EXCEPTION),

    /**
     * SYNTAX_EXCEPTION
     */
    SYNTAX_EXCEPTION(3001, SqlError.Code.SYNTAX_EXCEPTION),

    /**
     * ANALYZE_EXCEPTION
     */
    ANALYZE_EXCEPTION(3002, SqlError.Code.ANALYZE_EXCEPTION),

    /**
     * TYPE_ANALYZE_EXCEPTION
     */
    TYPE_ANALYZE_EXCEPTION(3003, SqlError.Code.TYPE_ANALYZE_EXCEPTION),

    /**
     * SYMBOL_ANALYZE_EXCEPTION
     */
    SYMBOL_ANALYZE_EXCEPTION(3004, SqlError.Code.SYMBOL_ANALYZE_EXCEPTION),

    /**
     * VALUE_ANALYZE_EXCEPTION
     */
    VALUE_ANALYZE_EXCEPTION(3005, SqlError.Code.VALUE_ANALYZE_EXCEPTION),

    /**
     * UNSUPPORTED_COMPILER_FEATURE_EXCEPTION
     */
    UNSUPPORTED_COMPILER_FEATURE_EXCEPTION(3010, SqlError.Code.UNSUPPORTED_COMPILER_FEATURE_EXCEPTION),
    
    /**
     * CC_EXCEPTION
     */
    CC_EXCEPTION(4000, SqlError.Code.CC_EXCEPTION),

    /**
     * OCC_EXCEPTION
     */
    OCC_EXCEPTION(4001, SqlError.Code.OCC_EXCEPTION),

    /**
     * OCC_READ_EXCEPTION
     */
    OCC_READ_EXCEPTION(4010, SqlError.Code.OCC_READ_EXCEPTION),

    /**
     * CONFLICT_ON_WRITE_PRESERVE_EXCEPTION
     */
    CONFLICT_ON_WRITE_PRESERVE_EXCEPTION(4015, SqlError.Code.CONFLICT_ON_WRITE_PRESERVE_EXCEPTION),

    /**
     * OCC_WRITE_EXCEPTION
     */
    OCC_WRITE_EXCEPTION(4011, SqlError.Code.OCC_WRITE_EXCEPTION),

    /**
     * LTX_EXCEPTION
     */
    LTX_EXCEPTION(4003, SqlError.Code.LTX_EXCEPTION),

    /**
     * LTX_READ_EXCEPTION
     */
    LTX_READ_EXCEPTION(4013, SqlError.Code.LTX_READ_EXCEPTION),
    
    /**
     * LTX_WRITE_EXCEPTION
     */
    LTX_WRITE_EXCEPTION(4014, SqlError.Code.LTX_WRITE_EXCEPTION),

    /**
     * RTX_EXCEPTION
     */
    RTX_EXCEPTION(4005, SqlError.Code.RTX_EXCEPTION),

    /**
     * BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION
     */
    BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION(4007, SqlError.Code.BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION),
    ;

    private final int codeNumber;

    private final SqlError.Code mapping;

    SqlServiceCode(int codeNumber, SqlError.Code mapping) {
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

    private static final EnumMap<SqlError.Code, SqlServiceCode> PROTO_MAPPING;
    static {
        Logger logger = LoggerFactory.getLogger(SqlServiceCode.class);
        PROTO_MAPPING = new EnumMap<>(SqlError.Code.class);
        for (var code : values()) {
            if (PROTO_MAPPING.putIfAbsent(code.mapping, code) != null) {
                logger.warn("conflict code mapping: {}.{}", //$NON-NLS-1$
                        SqlError.Code.class.getName(),
                        code.mapping.name());
            }
        }
        for (var proto : SqlError.Code.values()) {
            if (!PROTO_MAPPING.containsKey(proto)) {
                if (proto.equals(SqlError.Code.UNRECOGNIZED)) {
                    // UNRECOGNIZED is the enum value auto-generated by protocol buffer java library,
                    // so there should be no entry in the mapping.
                    continue;
                }
                logger.warn("unknown code mapping: {}.{}", //$NON-NLS-1$
                        SqlError.Code.class.getName(),
                        proto.name());
            }
        }
    }

    /**
     * Returns the corresponded diagnostic code.
     * @param code the original code
     * @return the corresponded diagnostic code, or {@link #SQL_SERVICE_EXCEPTION} if there is no suitable mapping
     */
    public static SqlServiceCode valueOf(SqlError.Code code) {
        return PROTO_MAPPING.getOrDefault(code, SQL_SERVICE_EXCEPTION);
    }
}
