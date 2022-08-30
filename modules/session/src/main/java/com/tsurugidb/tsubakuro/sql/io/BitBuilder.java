package com.tsurugidb.tsubakuro.sql.io;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Builds a bit sequence.
 */
public final class BitBuilder {

    private static final byte[] EMPTY = new byte[0];

    private byte[] buffer = EMPTY;

    private int size;

    /**
     * Creates a new instance.
     */
    public BitBuilder() {
        super();
    }

    /**
     * Creates a new instance.
     * @param contents the initial contents
     */
    public BitBuilder(@Nonnull boolean[] contents) {
        this(Objects.requireNonNull(contents), 0, contents.length);
    }

    /**
     * Creates a new instance.
     * @param contents the initial contents
     * @param offset the contents offset
     * @param length the contents length
     */
    public BitBuilder(@Nonnull boolean[] contents, int offset, int length) {
        Objects.requireNonNull(contents);
        Objects.checkFromIndexSize(offset, length, contents.length);
        setSize(length, false);
        for (int i = 0; i < length; i++) {
            set(i + offset, contents[i]);
        }
    }

    /**
     * Reserves the buffer capacity.
     * @param capacity the new capacity
     * @return this
     */
    public BitBuilder reserve(int capacity) {
        return reserve(capacity, true);
    }

    /**
     * Reserves the buffer capacity.
     * @param capacity the new capacity
     * @param keepContents {@code true} to keep the original buffer contents, {@code false} otherwise
     * @return this
     */
    public BitBuilder reserve(int capacity, boolean keepContents) {
        int byteCapacity = toByteSize(capacity);
        if (this.buffer.length < byteCapacity) {
            int sz = Math.max(byteCapacity, 16);
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
    public BitBuilder setSize(int newSize) {
        return setSize(newSize, true);
    }

    /**
     * Resizes the building contents.
     * @param newSize the new size of the building contents
     * @param keepContents {@code true} to keep the original buffer contents, {@code false} otherwise
     * @return this
     */
    public BitBuilder setSize(int newSize, boolean keepContents) {
        if (newSize < 0) {
            throw new IllegalArgumentException();
        }
        reserve(newSize, keepContents);
        if (keepContents) {
            clearBytes(size, newSize);
        }
        clearRestBits(newSize);
        this.size = newSize;
        return this;
    }

    private void clearBytes(int from, int to) {
        if (toByteSize(from) < toByteSize(to)) {
            assert toByteSize(to) <= buffer.length;
            Arrays.fill(buffer, toByteSize(from), toByteSize(to), (byte) 0);
        }
    }

    private void clearRestBits(int newSize) {
        int newBitOffset = toBitOffset(newSize);
        if (newBitOffset != 0) {
            int byteOffset = toByteOffset(newSize);
            assert byteOffset < buffer.length;
            buffer[byteOffset] &= ~(0xff << newBitOffset);
        }
    }

    /**
     * Returns whether or the bit on the position is set.
     * @param position the bit position
     * @return {@code true} if it is set, or {@code false} otherwise
     */
    public boolean get(int position) {
        Objects.checkIndex(position, size);
        var byteOffset = toByteOffset(position);
        var bitOffset = toBitOffset(position);
        return (buffer[byteOffset] & (1 << bitOffset)) != 0;
    }

    /**
     * Sets a bit into this bit sequence.
     * @param position the bit position
     * @param value {@code true} to set bit, or {@code false} to clear bit
     */
    public void set(int position, boolean value) {
        Objects.checkIndex(position, size);
        var byteOffset = toByteOffset(position);
        var bitOffset = toBitOffset(position);
        if (value) {
            buffer[byteOffset] |= 1 << bitOffset;
        } else {
            buffer[byteOffset] &= ~(1 << bitOffset);
        }
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
     * Returns the available bit count in the buffer.
     * @return the available bit count
     */
    public int getSize() {
        return size;
    }

    /**
     * Returns the available octet count in the buffer.
     * @return the available octet count
     */
    public int getByteSize() {
        return toByteSize(size);
    }

    /**
     * Builds a copy of the array with fitted length.
     * @return the copy
     */
    public boolean[] build() {
        var results = new boolean[size];
        for (int i = 0; i < size; i++) {
            results[i] = get(i);
        }
        return results;
    }

    @Override
    public String toString() {
        return Arrays.toString(build());
    }

    private static int toByteSize(int bitSize) {
        return (bitSize + 7) / 8;
    }

    private static int toByteOffset(int bitPosition) {
        return bitPosition / 8;
    }

    private static int toBitOffset(int bitPosition) {
        return bitPosition % 8;
    }
}
