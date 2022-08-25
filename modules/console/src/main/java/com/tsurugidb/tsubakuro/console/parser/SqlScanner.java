package com.tsurugidb.tsubakuro.console.parser;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Scans SQL compilation unit and splits it into segments of individual statements.
 */
class SqlScanner implements Closeable {

    private final SqlScannerFlex flex;

    /**
     * Creates a new instance.
     * @param input the input
     */
    SqlScanner(@Nonnull Reader input) {
        Objects.requireNonNull(input);
        flex = new SqlScannerFlex(input);
    }

    /**
     * Returns the next segment.
     * @return the next segment, or null if there are no more segments
     * @throws IOException if I/O error was occurred
     */
    Segment next() throws IOException {
        if (sawEof()) {
            return null;
        }
        while (true) {
            var saw = flex.yylex();
            if (saw == SqlScannerFlex.SAW_EOF || saw == SqlScannerFlex.SAW_DELIMITER) {
                return flex.build();
            }
        }
    }

    /**
     * Returns whether or not this scanner reached EOF.
     * @return {@code true} is reached EOF, otherwise {@code false}
     */
    boolean sawEof() {
        return flex.yyatEOF();
    }

    @Override
    public void close() throws IOException {
        flex.yyclose();
    }
}
