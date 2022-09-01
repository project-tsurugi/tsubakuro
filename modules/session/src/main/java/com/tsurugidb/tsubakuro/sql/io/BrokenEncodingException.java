package com.tsurugidb.tsubakuro.sql.io;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Exceptions for serialized data was broken.
 */
public class BrokenEncodingException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Represents error kind of {@link BrokenEncodingException}.
     */
    public enum Status {

        /**
         * Reached end of file unexpectedly.
         */
        UNEXPECTED_EOF,

        /**
         * Entry type is unrecognized.
         */
        UNRECOGNIZED_ENTRY_TYPE,

        /**
         * Entry type is not supported.
         */
        UNSUPPORTED_ENTRY_TYPE,

        /**
         * Value is out of range.
         */
        VALUE_OUT_OF_RANGE,
    }

    private final Status status;

    /**
     * Creates a new instance for unexpected EOF in input stream.
     * @return the created instance
     * @see Status#UNEXPECTED_EOF
     */
    public static BrokenEncodingException sawUnexpectedEof() {
        return new BrokenEncodingException(Status.UNEXPECTED_EOF,
                "input is interruptibly closed");
    }

    /**
     * Creates a new instance for unrecognized entry kind in input stream.
     * @param headerByte the header octet
     * @return the created instance
     * @see Status#UNRECOGNIZED_ENTRY_TYPE
     */
    public static BrokenEncodingException sawUnrecognizedEntry(int headerByte) {
        return new BrokenEncodingException(Status.UNRECOGNIZED_ENTRY_TYPE, MessageFormat.format(
                "unrecognized entry type: {0,integer}",
                headerByte));
    }

    /**
     * Creates a new instance for unsupported entry kind in input stream.
     * @param type the entry type
     * @return the created instance
     * @see Status#UNSUPPORTED_ENTRY_TYPE
     */
    public static BrokenEncodingException sawUnsupportedEntry(@Nonnull EntryType type) {
        Objects.requireNonNull(type);
        return new BrokenEncodingException(Status.UNSUPPORTED_ENTRY_TYPE, MessageFormat.format(
                "unsupported entry type: {0}",
                type));
    }

    /**
     * Creates a new instance for size field is too large in input stream.
     * @param size the actual size
     * @return the created instance
     * @see Status#VALUE_OUT_OF_RANGE
     */
    public static BrokenEncodingException sawUnsupportedSize(long size) {
        return new BrokenEncodingException(Status.VALUE_OUT_OF_RANGE, MessageFormat.format(
                "too large size: {0}",
                size));
    }

    /**
     * Creates a new instance for size field is too large in input stream.
     * @param value the actual value
     * @return the created instance
     * @see Status#VALUE_OUT_OF_RANGE
     */
    public static BrokenEncodingException sawSignedInt32OutOfRange(long value) {
        return new BrokenEncodingException(Status.VALUE_OUT_OF_RANGE, MessageFormat.format(
                "32-bit signed integer out of range: {0}",
                value));
    }

    /**
     * Creates a new instance.
     * @param status the error kind
     * @param message the error message
     */
    public BrokenEncodingException(@Nonnull Status status, @Nullable String message) {
        super(message, null);
        Objects.requireNonNull(status);
        this.status = status;
    }

    /**
     * Returns the error kind.
     * @return the error kind
     */
    public Status getStatus() {
        return status;
    }
}
