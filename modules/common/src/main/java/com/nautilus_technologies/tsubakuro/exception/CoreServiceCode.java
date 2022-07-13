package com.nautilus_technologies.tsubakuro.exception;

import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tateyama.proto.DiagnosticsProtos;

/**
 * Code of server core diagnostics.
 */
public enum CoreServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    UNKNOWN(0, DiagnosticsProtos.Code.UNKNOWN),

    /**
     * Unrecognized code.
     */
    UNRECOGNIZED(1, DiagnosticsProtos.Code.UNRECOGNIZED),

    /**
     * system is something wrong.
     */
    SYSTEM_ERROR(1_00, DiagnosticsProtos.Code.SYSTEM_ERROR),

    /**
     * operation is not supported.
     */
    UNSUPPORTED_OPERATION(1_01, DiagnosticsProtos.Code.UNSUPPORTED_OPERATION),

    /**
     * operation was requested in illegal or inappropriate time.
     */
    ILLEGAL_STATE(1_02, DiagnosticsProtos.Code.ILLEGAL_STATE),

    /**
     * I/O error was occurred.
     */
    IO_ERROR(1_03, DiagnosticsProtos.Code.IO_ERROR),

    /**
     * out of memory.
     */
    OUT_OF_MEMORY(1_04, DiagnosticsProtos.Code.OUT_OF_MEMORY),

    /**
     * authentication was failed.
     */
    AUTHENTICATION_ERROR(2_01, DiagnosticsProtos.Code.AUTHENTICATION_ERROR),

    /**
     * request is not permitted.
     */
    PERMISSION_ERROR(2_02, DiagnosticsProtos.Code.PERMISSION_ERROR),

    /**
     * access right has been expired.
     */
    ACCESS_EXPIRED(2_03, DiagnosticsProtos.Code.ACCESS_EXPIRED),

    /**
     * refresh right has been expired.
     */
    REFRESH_EXPIRED(2_04, DiagnosticsProtos.Code.REFRESH_EXPIRED),

    /**
     * credential information is broken.
     */
    BROKEN_CREDENTIAL(2_05, DiagnosticsProtos.Code.BROKEN_CREDENTIAL),

    /**
     * the current session is already closed.
     */
    SESSION_CLOSED(3_01, DiagnosticsProtos.Code.SESSION_CLOSED),

    /**
     * the current session is expired.
     */
    SESSION_EXPIRED(3_02, DiagnosticsProtos.Code.SESSION_EXPIRED),

    /**
     * the destination service was not found.
     */
    SERVICE_NOT_FOUND(4_01, DiagnosticsProtos.Code.SERVICE_NOT_FOUND),

    /**
     * the destination service was not found.
     */
    SERVICE_UNAVAILABLE(4_02, DiagnosticsProtos.Code.SERVICE_UNAVAILABLE),

    /**
     * request payload is not valid.
     */
    INVALID_REQUEST(5_01, DiagnosticsProtos.Code.INVALID_REQUEST),

    ;
    private final int codeNumber;

    private final DiagnosticsProtos.Code mapping;

    CoreServiceCode(int codeNumber, DiagnosticsProtos.Code mapping) {
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

    private static final EnumMap<DiagnosticsProtos.Code, CoreServiceCode> PROTO_MAPPING;
    static {
        Logger logger = LoggerFactory.getLogger(CoreServiceCode.class);
        PROTO_MAPPING = new EnumMap<>(DiagnosticsProtos.Code.class);
        for (var code : values()) {
            if (PROTO_MAPPING.putIfAbsent(code.mapping, code) != null) {
                logger.warn("conflict code mapping: {}.{}", //$NON-NLS-1$
                        DiagnosticsProtos.Code.class.getName(),
                        code.mapping.name());
            }
        }
        for (var proto : DiagnosticsProtos.Code.values()) {
            if (!PROTO_MAPPING.containsKey(proto)) {
                logger.warn("unknown code mapping: {}.{}", //$NON-NLS-1$
                        DiagnosticsProtos.Code.class.getName(),
                        proto.name());
            }
        }
    }

    /**
     * Returns the corresponded diagnostic code.
     * @param code the original code
     * @return the corresponded diagnostic code, or {@link #UNKNOWN} if there is no suitable mapping
     */
    public static CoreServiceCode valueOf(DiagnosticsProtos.Code code) {
        return PROTO_MAPPING.getOrDefault(code, UNRECOGNIZED);
    }
}
