package com.tsurugidb.tsubakuro.exception;

import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.diagnostics.proto.Diagnostics;

/**
 * Code of server core diagnostics.
 */
public enum CoreServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    UNKNOWN(0, Diagnostics.Code.UNKNOWN),

    /**
     * Unrecognized code.
     */
    UNRECOGNIZED(1, Diagnostics.Code.UNRECOGNIZED),

    /**
     * system is something wrong.
     */
    SYSTEM_ERROR(1_00, Diagnostics.Code.SYSTEM_ERROR),

    /**
     * operation is not supported.
     */
    UNSUPPORTED_OPERATION(1_01, Diagnostics.Code.UNSUPPORTED_OPERATION),

    /**
     * operation was requested in illegal or inappropriate time.
     */
    ILLEGAL_STATE(1_02, Diagnostics.Code.ILLEGAL_STATE),

    /**
     * I/O error was occurred.
     */
    IO_ERROR(1_03, Diagnostics.Code.IO_ERROR),

    /**
     * out of memory.
     */
    OUT_OF_MEMORY(1_04, Diagnostics.Code.OUT_OF_MEMORY),

    /**
     * reached server resource limit.
     */
    RESOURCE_LIMIT(1_05, Diagnostics.Code.RESOURCE_LIMIT),

    /**
     * authentication was failed.
     */
    AUTHENTICATION_ERROR(2_01, Diagnostics.Code.AUTHENTICATION_ERROR),

    /**
     * request is not permitted.
     */
    PERMISSION_ERROR(2_02, Diagnostics.Code.PERMISSION_ERROR),

    /**
     * access right has been expired.
     */
    ACCESS_EXPIRED(2_03, Diagnostics.Code.ACCESS_EXPIRED),

    /**
     * refresh right has been expired.
     */
    REFRESH_EXPIRED(2_04, Diagnostics.Code.REFRESH_EXPIRED),

    /**
     * credential information is broken.
     */
    BROKEN_CREDENTIAL(2_05, Diagnostics.Code.BROKEN_CREDENTIAL),

    /**
     * the current session is already closed.
     */
    SESSION_CLOSED(3_01, Diagnostics.Code.SESSION_CLOSED),

    /**
     * the current session is expired.
     */
    SESSION_EXPIRED(3_02, Diagnostics.Code.SESSION_EXPIRED),

    /**
     * the destination service was not found.
     */
    SERVICE_NOT_FOUND(4_01, Diagnostics.Code.SERVICE_NOT_FOUND),

    /**
     * the destination service was not found.
     */
    SERVICE_UNAVAILABLE(4_02, Diagnostics.Code.SERVICE_UNAVAILABLE),

    /**
     * request payload is not valid.
     */
    INVALID_REQUEST(5_01, Diagnostics.Code.INVALID_REQUEST),

    ;
    private final int codeNumber;

    private final Diagnostics.Code mapping;

    CoreServiceCode(int codeNumber, Diagnostics.Code mapping) {
        this.codeNumber = codeNumber;
        this.mapping = mapping;
    }

    /**
     * Structured code prefix of server diagnostics (`SCD-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "SCD"; //$NON-NLS-1$

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

    private static final EnumMap<Diagnostics.Code, CoreServiceCode> PROTO_MAPPING;
    static {
        Logger logger = LoggerFactory.getLogger(CoreServiceCode.class);
        PROTO_MAPPING = new EnumMap<>(Diagnostics.Code.class);
        for (var code : values()) {
            if (PROTO_MAPPING.putIfAbsent(code.mapping, code) != null) {
                logger.warn("conflict code mapping: {}.{}", //$NON-NLS-1$
                        Diagnostics.Code.class.getName(),
                        code.mapping.name());
            }
        }
        for (var proto : Diagnostics.Code.values()) {
            if (!PROTO_MAPPING.containsKey(proto)) {
                logger.warn("unknown code mapping: {}.{}", //$NON-NLS-1$
                        Diagnostics.Code.class.getName(),
                        proto.name());
            }
        }
    }

    /**
     * Returns the corresponded diagnostic code.
     * @param code the original code
     * @return the corresponded diagnostic code, or {@link #UNKNOWN} if there is no suitable mapping
     */
    public static CoreServiceCode valueOf(Diagnostics.Code code) {
        return PROTO_MAPPING.getOrDefault(code, UNRECOGNIZED);
    }
}
