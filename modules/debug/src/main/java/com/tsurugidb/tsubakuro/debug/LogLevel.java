package com.tsurugidb.tsubakuro.debug;

import javax.annotation.Nonnull;

import com.tsurugidb.debug.proto.DebugRequest;

/**
 * Represents a kind of log level on the server side.
 */
public enum LogLevel {

    /**
     * {@code INFO} level.
     */
    INFO(DebugRequest.Logging.Level.INFO),

    /**
     * {@code WARN} level.
     */
    WARN(DebugRequest.Logging.Level.WARN),

    /**
     * {@code ERROR} level.
     */
    ERROR(DebugRequest.Logging.Level.ERROR),

    ;

    private DebugRequest.Logging.Level mapping;

    LogLevel(@Nonnull DebugRequest.Logging.Level mapping) {
        assert mapping != null;
        this.mapping = mapping;
    }

    /**
     * Returns the corresponded representation in protocol buffer.
     * @return the corresponded value
     */
    public DebugRequest.Logging.Level getMapping() {
        return mapping;
    }
}
