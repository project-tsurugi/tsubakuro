package com.tsurugidb.tsubakuro.console.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class SqlScannerTest {

    @Test
    void empty_script() throws Exception {
        var s = scanOne();
        assertEquals(0, s.getOffset());
        assertEquals("", s.getText());

        assertEquals(1, s.getTokens().size());
        assertEquals(List.of(TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void empty_statement() throws Exception {
        var ss = scan(";");
        assertEquals(2, ss.size());
        {
            var s = ss.get(0);
            assertEquals(List.of(TokenKind.END_OF_STATEMENT), kinds(s));
        }
        {
            var s = ss.get(1);
            assertEquals(List.of(TokenKind.END_OF_STATEMENT), kinds(s));
        }
    }

    @Test
    void simple_dml() throws Exception {
        var s = scanOne("SELECT * FROM TBL");
        assertEquals(List.of(
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.ASTERISK,
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.END_OF_STATEMENT), kinds(s));
        assertEquals("SELECT * FROM TBL", s.getText());
    }

    @Test
    void unhandled_simple() throws Exception {
        var s = scanOne("%%% Hello, world!");
        assertEquals(List.of(
                TokenKind.UNHANDLED_TEXT,
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.COMMA,
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.UNHANDLED_TEXT,
                TokenKind.END_OF_STATEMENT), kinds(s));
        assertEquals("%%% Hello, world!", s.getText());
    }

    @Test
    void regular_identifier() throws Exception {
        var s = scanOne("hello_1");
        assertEquals(List.of(
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void delimited_identifier() throws Exception {
        var s = scanOne("\"Hello, \\\"world\\\"!\"");
        assertEquals(List.of(
                TokenKind.DELIMITED_IDENTIFIER,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void numeric_literal_integer() throws Exception {
        var s = scanOne("123");
        assertEquals(List.of(
                TokenKind.NUMERIC_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void numeric_literal_real() throws Exception {
        var s = scanOne("1.0 .1 1.");
        assertEquals(List.of(
                TokenKind.NUMERIC_LITERAL,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void numeric_literal_exponent() throws Exception {
        var s = scanOne("1E1 1.0E+1 .1E-1 1.e0");
        assertEquals(List.of(
                TokenKind.NUMERIC_LITERAL,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void character_string_literal() throws Exception {
        var s = scanOne("'aaa' 'ab\\\'c'");
        assertEquals(List.of(
                TokenKind.CHARACTER_STRING_LITERAL,
                TokenKind.CHARACTER_STRING_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void binary_string_literal() throws Exception {
        var s = scanOne("X'01' x'ab\\\'cd'");
        assertEquals(List.of(
                TokenKind.BINARY_STRING_LITERAL,
                TokenKind.BINARY_STRING_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void boolean_literal() throws Exception {
        var s = scanOne("TRUE False");
        assertEquals(List.of(
                TokenKind.BOOLEAN_LITERAL,
                TokenKind.BOOLEAN_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void null_literal() throws Exception {
        var s = scanOne("NULL Null");
        assertEquals(List.of(
                TokenKind.NULL_LITERAL,
                TokenKind.NULL_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
    }

    @Test
    void special_command() throws Exception {
        var ss = scan("\\EXIT \\Halt");
        assertEquals(3, ss.size());
        {
            var s = ss.get(0);
            assertEquals(List.of(TokenKind.SPECIAL_COMMAND), kinds(s));
        }
        {
            var s = ss.get(1);
            assertEquals(List.of(TokenKind.SPECIAL_COMMAND), kinds(s));
        }
        {
            var s = ss.get(2);
            assertEquals(List.of(TokenKind.END_OF_STATEMENT), kinds(s));
        }
    }

    @Test
    void block_comment() throws Exception {
        var s = scanOne(
                "SELECT",
                "/*",
                " * TESTING",
                " */ 1");
        assertEquals(List.of(
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
        assertEquals(1, s.getComments().size());
        assertEquals(TokenKind.BLOCK_COMMENT, s.getComments().get(0).getKind());
    }

    @Test
    void slash_comment() throws Exception {
        var s = scanOne(
                "// just returns 1",
                "SELECT 1");
        assertEquals(List.of(
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
        assertEquals(1, s.getComments().size());
        assertEquals(TokenKind.SLASH_COMMENT, s.getComments().get(0).getKind());
    }

    @Test
    void hyphen_comment() throws Exception {
        var s = scanOne(
                "-- just returns 1",
                "SELECT 1");
        assertEquals(List.of(
                TokenKind.REGULAR_IDENTIFIER,
                TokenKind.NUMERIC_LITERAL,
                TokenKind.END_OF_STATEMENT), kinds(s));
        assertEquals(1, s.getComments().size());
        assertEquals(TokenKind.HYPHEN_COMMENT, s.getComments().get(0).getKind());
    }

    @Test
    void punctuations() throws Exception {
        var ss = scan(".,;()+-*");
        assertEquals(2, ss.size());
        {
            var s = ss.get(0);
            assertEquals(List.of(
                    TokenKind.DOT,
                    TokenKind.COMMA,
                    TokenKind.END_OF_STATEMENT), kinds(s));
        }
        {
            var s = ss.get(1);
            assertEquals(List.of(
                    TokenKind.LEFT_PAREN,
                    TokenKind.RIGHT_PAREN,
                    TokenKind.PLUS,
                    TokenKind.MINUS,
                    TokenKind.ASTERISK,
                    TokenKind.END_OF_STATEMENT), kinds(s));
        }
    }

    private static List<TokenKind> kinds(Segment segment) {
        return segment.getTokens().stream().map(TokenInfo::getKind).collect(Collectors.toList());
    }

    private static Segment scanOne(String... lines) throws IOException {
        List<Segment> segments = scan(lines);
        assertEquals(1, segments.size());
        return segments.get(0);
    }

    private static List<Segment> scan(String... lines) throws IOException {
        List<Segment> segments = new ArrayList<>();
        try (var scanner = new SqlScanner(new StringReader(String.join("\n", lines)))) {
            while (true) {
                var s = scanner.next();
                if (s == null) {
                    break;
                }
                segments.add(s);
            }
        }
        return segments;
    }
}
