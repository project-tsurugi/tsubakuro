package com.nautilus_technologies.tsubakuro.low.sql.io;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Builds an array of bytes.
 */
public final class ByteBuilder {

    private static final byte[] EMPTY = new byte[0];

    private byte[] buffer = EMPTY;

    private int size;

    /**
     * Creates a new instance.
     */
    public ByteBuilder() {
        this.size = 0;
    }

    /**
     * Creates a new instance.
     * @param contents the initial contents
     */
    public ByteBuilder(@Nonnull byte[] contents) {
        this(Objects.requireNonNull(contents), 0, contents.length);
    }

    /**
     * Creates a new instance.
     * @param contents the initial contents
     * @param offset the contents offset
     * @param length the contents length
     */
    public ByteBuilder(@Nonnull byte[] contents, int offset, int length) {
        Objects.requireNonNull(contents);
        Objects.checkFromIndexSize(offset, length, contents.length);
        setSize(length, false);
        System.arraycopy(contents, offset, this.buffer, 0, length);
    }

    /**
     * Reserves the buffer capacity.
     * @param capacity the new capacity
     * @return this
     */
    public ByteBuilder reserve(int capacity) {
        return reserve(capacity, true);
    }

    /**
     * Reserves the buffer capacity.
     * @param capacity the new capacity
     * @param keepContents {@code true} to keep the original buffer contents, {@code false} otherwise
     * @return this
     */
    public ByteBuilder reserve(int capacity, boolean keepContents) {
        if (this.buffer.length < capacity) {
            int sz = Math.max(capacity, 16);
            if (keepContents) {
                this.buffer = Arrays.copyOf(this.buffer, sz);
            } else {
                this.buffer = new byte[sz];
            }
        }
        return this;
    }

    /**
     * Resizes the building contents.
     * @param newSize the new size of the building contents
     * @return this
     */
    public ByteBuilder setSize(int newSize) {
        return setSize(newSize, true);
    }

    /**
     * Resizes the building contents.
     * @param newSize the new size of the building contents
     * @param keepContents {@code true} to keep the original buffer contents, {@code false} otherwise
     * @return this
     */
    public ByteBuilder setSize(int newSize, boolean keepContents) {
        if (newSize < 0) {
            throw new IllegalArgumentException();
        }
        reserve(newSize, keepContents);
        if (this.size < newSize && keepContents) {
            Arrays.fill(this.buffer, this.size, newSize, (byte) 0);
        }
        this.size = newSize;
        return this;
    }

    /**
     * Returns whether or the byte on the position is set.
     * @param position the byte position
     * @return the value
     */
    public byte get(int position) {
        Objects.checkIndex(position, size);
        return buffer[position];
    }

    /**
     * Sets a bit into this byte sequence.
     * @param position the byte position
     * @param value {@code true} to set bit, or {@code false} to clear bit
     */
    public void set(int position, int value) {
        Objects.checkIndex(position, size);
        buffer[position] = (byte) value;
    }

    /**
     * Returns the buffer array.
     * @return the buffer array
     */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP",
            justification = "don't take copies for optimization")
    public byte[] getData() {
        return buffer;
    }

    /**
     * Returns the available size in the buffer.
     * @return the available size
     */
    public int getSize() {
        return size;
    }

    /**
     * Builds a copy of the array with fitted length.
     * @return the copy
     */
    public byte[] build() {
        return Arrays.copyOf(buffer, size);
    }

    @Override
    public String toString() {
        return Arrays.toString(build());
    }
}
