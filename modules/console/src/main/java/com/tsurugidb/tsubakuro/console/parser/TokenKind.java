package com.tsurugidb.tsubakuro.console.parser;

enum TokenKind {
    /**
     * End-Of-File.
     */
    EOF(true),

    /**
     * text segment which the parser cannot handle.
     */
    UNHANDLED_TEXT,

    /**
     * regular (unquoted) identifiers.
     */
    REGULAR_IDENTIFIER,

    /**
     * delimited identifiers.
     */
    DELIMITED_IDENTIFIER,

    // literals

    /**
     * numeric literals.
     */
    NUMERIC_LITERAL,

    /**
     * true.
     */
    TRUE_LITERAL,

    /**
     * false.
     */
    FALSE_LITERAL,

    /**
     * null.
     */
    NULL_LITERAL,

    /**
     * character string literals.
     */
    CHARACTER_STRING_LITERAL,

    /**
     * binary string literals.
     */
    BINARY_STRING_LITERAL,

    // punctuation

    /**
     * dot.
     */
    DOT,

    /**
     * comma.
     */
    COMMA,

    /**
     * semicolon.
     */
    SEMICOLON(true),

    /**
     * open paren.
     */
    LEFT_PAREN,

    /**
     * close paren.
     */
    RIGHT_PAREN,

    // operators

    /**
     * plus sign.
     */
    PLUS,

    /**
     * minus sign.
     */
    MINUS,

    /**
     * asterisk.
     */
    ASTERISK,

    /**
     * equal sign.
     */
    EQUAL,

    /**
     * special commands.
     */
    SPECIAL_COMMAND(true),

    // comments

    /**
     * C-style comment block.
     */
    BLOCK_COMMENT,

    /**
     * comments leading two slashes.
     */
    SLASH_COMMENT,

    /**
     * comments leading two hyphens.
     */
    HYPHEN_COMMENT,

    /**
     * pseudo-symbol of end of statement.
     */
    END_OF_STATEMENT,

    ;
    private final boolean statementDelimiter;

    TokenKind() {
        statementDelimiter = false;
    }

    TokenKind(boolean statementDelimiter) {
        this.statementDelimiter = statementDelimiter;
    }

    /**
     * Returns whether or not the token is a statement delimiter.
     * @return whether or not the token is a statement delimiter
     */
    public boolean isStatementDelimiter() {
        return statementDelimiter;
    }
}
