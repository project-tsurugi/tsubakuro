package com.tsurugidb.tsubakuro.console.parser;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Represents a token.
 */
class TokenInfo {

    private final TokenKind kind;

    private final int offset;

    private final int length;

    private final int startLine;

    private final int startColumn;

    /**
     * Creates a new instance.
     * @param kind the token kind
     * @param offset the token starting offset in the segment (0-origin)
     * @param length the number of characters in this token
     * @param startLine the starting line in the document (0-origin)
     * @param startColumn the starting column in the line (0-origin)
     */
    TokenInfo(@Nonnull TokenKind kind, int offset, int length, int startLine, int startColumn) {
        Objects.requireNonNull(kind);
        this.kind = kind;
        this.offset = offset;
        this.length = length;
        this.startLine = startLine;
        this.startColumn = startColumn;
    }

    /**
     * Returns the token kind.
     * @return the token kind
     */
    TokenKind getKind() {
        return kind;
    }

    /**
     * Returns the token offset.
     * @return the offset (0-origin)
     */
    int getOffset() {
        return offset;
    }

    /**
     * Returns the token length.
     * @return the length
     */
    int getLength() {
        return length;
    }

    /**
     * Returns the starting line in the document.
     * @return the starting line number (0-origin)
     */
    int getStartLine() {
        return startLine;
    }

    /**
     * Returns the starting column number in the line.
     * @return the starting column number (0-origin)
     */
    int getStartColumn() {
        return startColumn;
    }
}
