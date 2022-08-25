package com.tsurugidb.tsubakuro.console.parser;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.console.model.ErroneousStatement;
import com.tsurugidb.tsubakuro.console.model.Region;
import com.tsurugidb.tsubakuro.console.model.Statement;

/**
 * Parses list of SQL statements and returns corresponding {@link Statement} objects.
 */
public class SqlParser implements Closeable {

    private final SqlScanner scanner;

    /**
     * Creates a new instance.
     * @param input the input source
     */
    public SqlParser(@Nonnull Reader input) {
        Objects.requireNonNull(input);
        this.scanner = new SqlScanner(input);
    }

    /**
     * Consumes the next statement.
     * @return the next statement, or {@code null} after reached EOF
     * @throws IOException if I/O error was occurred
     */
    public Statement next() throws IOException {
        var segment = scanner.next();
        if (segment == null) {
            return null;
        }
        if (scanner.sawEof()
                && segment.getTokens().size() == 1
                && segment.getTokens().get(0).getKind() == TokenKind.END_OF_STATEMENT
                && segment.getComments().isEmpty()) {
            return null;
        }
        try {
            return SegmentAnalyzer.analyze(segment);
        } catch (ParseException e) {
            return new ErroneousStatement(
                    segment.getText(),
                    new Region(
                            segment.getOffset(),
                            segment.getText().length(),
                            segment.getStartLine(),
                            segment.getStartColumn()),
                    e.getErrorKind(),
                    e.getOccurrence(),
                    e.getLocalizedMessage());
        }
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }
}
