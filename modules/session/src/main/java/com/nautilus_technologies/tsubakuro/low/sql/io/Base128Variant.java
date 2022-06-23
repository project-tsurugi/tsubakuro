package com.nautilus_technologies.tsubakuro.low.sql.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Utilities about variable length integers.
 */
public final class Base128Variant {

    /**
     * Writes variable length unsigned integer into the specified stream.
     * @param value the value
     * @param output the target output
     * @return the number of written bytes
     * @throws IOException if I/O error while writing byte sequence to the output
     */
    public static int writeUnsigned(long value, @Nonnull OutputStream output) throws IOException {
        Objects.requireNonNull(output);

        long v = value;
        for (int i = 0; i < 8; i++) {
            // retrieve the next 7-bit group
            int group = (int) (v & 0x7f);
            v >>>= 7;

            if (v == 0) {
                // end of contents
                output.write(group);
                return i + 1;
            }

            // continue contents
            output.write(0x80 | group);
        }

        // write the last 8-bit group
        assert v != 0;
        assert v <= 0xff;
        output.write((int) v);
        return 9;
    }

    /**
     * Writes variable length signed integer into the specified stream.
     * @param value the value
     * @param output the target output
     * @return the number of written bytes
     * @throws IOException if I/O error while writing byte sequence to the output
     */
    public static int writeSigned(long value, @Nonnull OutputStream output) throws IOException {
        Objects.requireNonNull(output);
        return writeUnsigned((value << 1) ^ (value >> 63), output);
    }

    /**
     * Reads a variable length unsigned integer from the specified stream.
     * @param input the source input
     * @return the read value
     * @throws BrokenEncodingException if reached unexpected EOF
     * @throws IOException if I/O error while reading byte sequence from the input
     */
    public static long readUnsigned(@Nonnull InputStream input) throws IOException {
        Objects.requireNonNull(input);

        long result = 0;
        for (int i = 0; i < 8; i++) {
            int c = input.read();
            checkEof(c);
            // retrieves the next 7-bit group
            result |= (long) (c & 0x7f) << (i * 7);

            // end of contents
            if ((c & 0x80) == 0) {
                return result;
            }

            // continue contents
        }

        // retrieves the last 8-bit group
        int c = input.read();
        checkEof(c);
        result |= (long) c << 56;
        return result;
    }

    private static void checkEof(int c) throws BrokenEncodingException {
        if (c < 0) {
            throw BrokenEncodingException.sawUnexpectedEof();
        }
    }

    /**
     * Reads a variable length signed integer from the specified stream.
     * @param input the source input
     * @return the read value
     * @throws BrokenEncodingException if reached unexpected EOF
     * @throws IOException if I/O error while reading byte sequence from the input
     */
    public static long readSigned(@Nonnull InputStream input) throws IOException {
        Objects.requireNonNull(input);
        long v = readUnsigned(input);
        return (v & 0x01) == 0 ? (v >>> 1) : ~(v >>> 1);
    }

    private Base128Variant() {
        throw new AssertionError();
    }
}
