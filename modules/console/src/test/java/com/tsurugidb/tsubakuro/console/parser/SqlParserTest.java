package com.tsurugidb.tsubakuro.console.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.console.model.Statement;

class SqlParserTest {

    @Test
    void simple() throws Exception {
        var ss = parse("SELECT * FROM T");
        assertEquals(1, ss.size());
        assertEquals(Statement.Kind.GENERIC, ss.get(0).getKind());
        assertEquals("SELECT * FROM T", ss.get(0).getText());
    }

    @Test
    void multiple() throws Exception {
        var ss = parse("SELECT * FROM T0; SELECT * FROM T1; SELECT * FROM T2");
        assertEquals(3, ss.size());

        assertEquals(Statement.Kind.GENERIC, ss.get(0).getKind());
        assertEquals("SELECT * FROM T0", ss.get(0).getText());

        assertEquals(Statement.Kind.GENERIC, ss.get(1).getKind());
        assertEquals("SELECT * FROM T1", ss.get(1).getText());

        assertEquals(Statement.Kind.GENERIC, ss.get(2).getKind());
        assertEquals("SELECT * FROM T2", ss.get(2).getText());
    }

    @Test
    void error() throws Exception {
        var ss = parse("ROLLBACK FROM T");
        assertEquals(1, ss.size());
        assertEquals(Statement.Kind.ERRONEOUS, ss.get(0).getKind());
    }

    @Test
    void empty_input() throws Exception {
        var ss = parse("");
        assertEquals(0, ss.size());
    }

    @Test
    void empty_statement() throws Exception {
        var ss = parse(";");
        assertEquals(1, ss.size());
        assertEquals(Statement.Kind.EMPTY, ss.get(0).getKind());
    }

    private static List<Statement> parse(String text) throws IOException {
        List<Statement> results = new ArrayList<>();
        try (var parser = new SqlParser(new StringReader(text))) {
            while (true) {
                var s = parser.next();
                if (s == null) {
                    break;
                }
                results.add(s);
            }
        }
        return results;
    }
}
