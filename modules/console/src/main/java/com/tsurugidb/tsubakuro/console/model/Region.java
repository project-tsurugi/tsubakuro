package com.tsurugidb.tsubakuro.console.model;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents region of a text snippet.
 */
public class Region {

    private final long position;

    private final int size;

    private final int startLine;

    private final int startColumn;

    /**
     * Creates a new instance.
     * @param position the starting position in the document (0-origin)
     * @param size the length of this contents
     * @param startLine the starting line number in the document (0-origin)
     * @param startColumn the starting column number in the document (0-origin)
     */
    public Region(long position, int size, int startLine, int startColumn) {
        this.position = position;
        this.size = size;
        this.startLine = startLine;
        this.startColumn = startColumn;
    }

    /**
     * Wraps a value as {@link Regioned}.
     * @param <V> the value type
     * @param value the value
     * @return the wrapped value
     */
    public <V> Regioned<V> wrap(@Nullable V value) {
        return new Regioned<>(value, this);
    }

    /**
     * Returns the starting position in the document (0-origin).
     * @return the position
     */
    public long getPosition() {
        return position;
    }

    /**
     * Returns the number of characters in this contents.
     * @return the size
     */
    public int getSize() {
        return size;
    }

    /**
     * the starting line number in the document (0-origin).
     * @return the line number
     */
    public int getStartLine() {
        return startLine;
    }

    /**
     * the starting column number in the document (0-origin).
     * @return the column number
     */
    public int getStartColumn() {
        return startColumn;
    }

    /**
     * Concatenates this and the given region.
     * @param other the region to concatenate
     * @return the concatenated region
     */
    public Region union(@Nonnull Region other) {
        Objects.requireNonNull(other);
        if (position < other.position) {
            return union0(this, other);
        }
        return union0(other, this);
    }

    private static Region union0(@Nonnull Region first, @Nonnull Region second) {
        return new Region(
                first.position,
                (int) (second.position - first.position) + second.size,
                first.startLine,
                first.startColumn);
    }

    @Override
    public String toString() {
        return String.format(
                "Region(position=%s, size=%s, line=%s, column=%s)", //$NON-NLS-1$
                position,
                size,
                startLine,
                startColumn);
    }
}
