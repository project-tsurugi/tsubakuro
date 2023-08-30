package com.tsurugidb.tsubakuro.sql;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.exception.*;

/**
 * An exception which occurs if Tsurugi OLTP server core is something wrong.
 */
public class SqlServiceException extends ServerException {

    private static final long serialVersionUID = 1L;

    private final SqlServiceCode code;

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     * @param cause the original cause
     */
    public SqlServiceException(@Nonnull SqlServiceCode code, @Nullable String message, @Nullable Throwable cause) {
        super(buildMessage(code, message), cause);
        this.code = code;
    }

    private static String buildMessage(SqlServiceCode code, @Nullable String message) {
        Objects.requireNonNull(code);
        if (message == null || message.isEmpty()) {
            return String.format("%s: %s", code.getStructuredCode(), code.name());
        }
        return String.format("%s: %s", code.getStructuredCode(), message); //$NON-NLS-1$
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     */
    public SqlServiceException(@Nonnull SqlServiceCode code) {
        this(code, null, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     */
    public SqlServiceException(@Nonnull SqlServiceCode code, @Nullable String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param cause the original cause
     */
    public SqlServiceException(@Nonnull SqlServiceCode code, @Nullable Throwable cause) {
        this(code, null, cause);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     * @param cause the original cause
     */
    public static SqlServiceException of(@Nonnull SqlServiceCode code, @Nullable String message, @Nullable Throwable cause) {
        switch (code) {
        case SQL_SERVICE_EXCEPTION: return new SqlServiceException(code, message, cause);
        case SQL_EXECUTION_EXCEPTION: return new SqlExecutionException(code, message, cause);
        case CONSTRAINT_VIOLATION_EXCEPTION: return new ConstraintViolationException(code, message, cause);
        case UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION: return new UniqueConstraintViolationException(code, message, cause);
        case NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION: return new NotNullConstraintViolationException(code, message, cause);
        case REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION: return new ReferentialIntegrityConstraintViolationException(code, message, cause);
        case CHECK_CONSTRAINT_VIOLATION_EXCEPTION: return new CheckConstraintViolationException(code, message, cause);
        case EVALUATION_EXCEPTION: return new EvaluationException(code, message, cause);
        case VALUE_EVALUATION_EXCEPTION: return new ValueEvaluationException(code, message, cause);
        case SCALAR_SUBQUERY_EVALUATION_EXCEPTION: return new ScalarSubqueryEvaluationException(code, message, cause);
        case TARGET_NOT_FOUND_EXCEPTION: return new TargetNotFoundException(code, message, cause);
        case TARGET_ALREADY_EXISTS_EXCEPTION: return new TargetAlreadyExistsException(code, message, cause);
        case INCONSISTENT_STATEMENT_EXCEPTION: return new InconsistentStatementException(code, message, cause);
        case RESTRICTED_OPERATION_EXCEPTION: return new RestrictedOperationException(code, message, cause);
        case DEPENDENCIES_VIOLATION_EXCEPTION: return new DependenciesViolationException(code, message, cause);
        case WRITE_OPERATION_BY_RTX_EXCEPTION: return new WriteOperationByRtxException(code, message, cause);
        case LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION: return new LtxWriteOperationWithoutWritePreserveException(code, message, cause);
        case READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION: return new ReadOperationOnRestrictedReadAreaException(code, message, cause);
        case INACTIVE_TRANSACTION_EXCEPTION: return new InactiveTransactionException(code, message, cause);
        case PARAMETER_EXCEPTION: return new ParameterException(code, message, cause);
        case UNRESOLVED_PLACEHOLDER_EXCEPTION: return new UnresolvedPlaceholderException(code, message, cause);
        case LOAD_FILE_EXCEPTION: return new LoadFileException(code, message, cause);
        case LOAD_FILE_NOT_FOUND_EXCEPTION: return new LoadFileNotFoundException(code, message, cause);
        case LOAD_FILE_FORMAT_EXCEPTION: return new LoadFileFormatException(code, message, cause);
        case DUMP_FILE_EXCEPTION: return new DumpFileException(code, message, cause);
        case DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION: return new DumpDirectoryInaccessibleException(code, message, cause);
        case SQL_LIMIT_REACHED_EXCEPTION: return new SqlLimitReachedException(code, message, cause);
        case TRANSACTION_EXCEEDED_LIMIT_EXCEPTION: return new TransactionExceededLimitException(code, message, cause);
        case SQL_REQUEST_TIMEOUT_EXCEPTION: return new SqlRequestTimeoutException(code, message, cause);
        case DATA_CORRUPTION_EXCEPTION: return new DataCorruptionException(code, message, cause);
        case SECONDARY_INDEX_CORRUPTION_EXCEPTION: return new SecondaryIndexCorruptionException(code, message, cause);
        case REQUEST_FAILURE_EXCEPTION: return new RequestFailureException(code, message, cause);
        case TRANSACTION_NOT_FOUND_EXCEPTION: return new TransactionNotFoundException(code, message, cause);
        case STATEMENT_NOT_FOUND_EXCEPTION: return new StatementNotFoundException(code, message, cause);
        case INTERNAL_EXCEPTION: return new InternalException(code, message, cause);
        case UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION: return new UnsupportedRuntimeFeatureException(code, message, cause);
        case BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION: return new BlockedByHighPriorityTransactionException(code, message, cause);
        case COMPILE_EXCEPTION: return new CompileException(code, message, cause);
        case SYNTAX_EXCEPTION: return new SyntaxException(code, message, cause);
        case ANALYZE_EXCEPTION: return new AnalyzeException(code, message, cause);
        case TYPE_ANALYZE_EXCEPTION: return new TypeAnalyzeException(code, message, cause);
        case SYMBOL_ANALYZE_EXCEPTION: return new SymbolAnalyzeException(code, message, cause);
        case VALUE_ANALYZE_EXCEPTION: return new ValueAnalyzeException(code, message, cause);
        case UNSUPPORTED_COMPILER_FEATURE_EXCEPTION: return new UnsupportedCompilerFeatureException(code, message, cause);
        case CC_EXCEPTION: return new CcException(code, message, cause);
        case OCC_EXCEPTION: return new OccException(code, message, cause);
        case OCC_READ_EXCEPTION: return new OccReadException(code, message, cause);
        case CONFLICT_ON_WRITE_PRESERVE_EXCEPTION: return new ConflictOnWritePreserveException(code, message, cause);
        case OCC_WRITE_EXCEPTION: return new OccWriteException(code, message, cause);
        case LTX_EXCEPTION: return new LtxException(code, message, cause);
        case LTX_READ_EXCEPTION: return new LtxReadException(code, message, cause);
        case LTX_WRITE_EXCEPTION: return new LtxWriteException(code, message, cause);
        case RTX_EXCEPTION: return new RtxException(code, message, cause);
        case BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION: return new BlockedByConcurrentOperationException(code, message, cause);
        }
        return new SqlServiceException(code, "code given is undefined: " + message, cause);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     */
    public static SqlServiceException of(@Nonnull SqlServiceCode code) {
        return of(code, null, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     */
    public static SqlServiceException of(@Nonnull SqlServiceCode code, @Nullable String message) {
        return of(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param cause the original cause
     */
    public static SqlServiceException of(@Nonnull SqlServiceCode code, @Nullable Throwable cause) {
        return of(code, null, cause);
    }

    @Override
    public SqlServiceCode getDiagnosticCode() {
        return code;
    }
}
