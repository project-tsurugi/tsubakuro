package com.tsurugidb.tsubakuro.console.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.console.model.CallStatement;
import com.tsurugidb.tsubakuro.console.model.CommitStatement;
import com.tsurugidb.tsubakuro.console.model.CommitStatement.CommitStatus;
import com.tsurugidb.tsubakuro.console.model.ErroneousStatement.ErrorKind;
import com.tsurugidb.tsubakuro.console.model.Regioned;
import com.tsurugidb.tsubakuro.console.model.SpecialStatement;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ExclusiveMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ReadWriteMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.TransactionMode;
import com.tsurugidb.tsubakuro.console.model.Statement;
import com.tsurugidb.tsubakuro.console.model.Value;

class SegmentAnalyzerTest {

    @Test
    void simple() throws Exception {
        Statement s = analyze("SELECT * FROM TBL");
        assertEquals(Statement.Kind.GENERIC, s.getKind());
        assertEquals("SELECT * FROM TBL", s.getText());
    }

    @Test
    void empty_eof() throws Exception {
        Statement s = analyze("");
        assertEquals(Statement.Kind.EMPTY, s.getKind());
        assertEquals("", s.getText());
    }

    @Test
    void empty_semicolon() throws Exception {
        Statement s = analyze(";");
        assertEquals(Statement.Kind.EMPTY, s.getKind());
        assertEquals("", s.getText());
    }

    @Test
    void start_transaction() throws Exception {
        Statement s = analyze("START TRANSACTION");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals("START TRANSACTION", s.getText());
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void begin() throws Exception {
        Statement s = analyze("BEGIN");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals("BEGIN", s.getText());
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void begin_transaction() throws Exception {
        Statement s = analyze("BEGIN TRANSACTION");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals("BEGIN TRANSACTION", s.getText());
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_long_transaction() throws Exception {
        Statement s = analyze("START LONG TRANSACTION");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(TransactionMode.LONG, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_read_only() throws Exception {
        Statement s = analyze("START TRANSACTION READ ONLY");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(ReadWriteMode.READ_ONLY, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_read_only_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION READ ONLY DEFERRABLE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(ReadWriteMode.READ_ONLY_DEFERRABLE, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_read_write() throws Exception {
        Statement s = analyze("START TRANSACTION READ WRITE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(ReadWriteMode.READ_WRITE, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_empty() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE ()");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of(), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_identifier() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_delimited() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE \"Hello, world!\"");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("\"Hello, world!\""), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_identifier_chain() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a.b.c");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a.b.c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_glob() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a.b.*");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a.b.*"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_string() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE 'a.b.c'");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a.b.c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_multiple() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a, b, c");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a", "b", "c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_write_preserve_multiple_paren() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE (a, b, c)");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a", "b", "c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_read_area_include() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA INCLUDE a");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_read_area_exclude() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA EXCLUDE a");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_read_area_include_exclude() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA INCLUDE a EXCLUDE b");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("b"), unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_read_area_exclude_include() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA EXCLUDE a INCLUDE b");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(List.of("b"), unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_prior() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE PRIOR");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.PRIOR_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_prior_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE PRIOR DEFERRABLE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.PRIOR_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_prior_immediate() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE PRIOR IMMEDIATE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.PRIOR_IMMEDIATE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_excluding() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDING");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_excluding_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDING DEFERRABLE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_excluding_immediate() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDING IMMEDIATE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_IMMEDIATE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_exclude() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_exclude_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDE DEFERRABLE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_execute_exclude_immediate() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDE IMMEDIATE");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_IMMEDIATE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_as() throws Exception {
        Statement s = analyze("START TRANSACTION AS tx1");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals("tx1", unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_as_string() throws Exception {
        Statement s = analyze("START TRANSACTION AS 'Hello, world!'");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals("Hello, world!", unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_empty() throws Exception {
        Statement s = analyze("START TRANSACTION WITH ()");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of(), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_key() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of()), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_key_value() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=1");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of(1)), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_multiple() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=1, b=2, c=3");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(
                Map.of("a", Value.of(1), "b", Value.of(2), "c", Value.of(3)),
                unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_parenthesized() throws Exception {
        Statement s = analyze("START TRANSACTION WITH (a=1)");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of(1)), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_parenthesized_multiple() throws Exception {
        Statement s = analyze("START TRANSACTION WITH (a=1, b=2, c=3)");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(
                Map.of("a", Value.of(1), "b", Value.of(2), "c", Value.of(3)),
                unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_key_qualified() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a.b.c = 0");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a.b.c", Value.of(0)), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_key_character_string() throws Exception {
        Statement s = analyze("START TRANSACTION WITH 'a'='b'");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of("b")), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_value_identifier() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=testing");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of("testing")), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_LTX() throws Exception {
        Statement s = analyze("START LONG TRANSACTION WRITE PRESERVE t1 READ AREA EXCLUDE t2 AS t3;");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(TransactionMode.LONG, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("t1"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("t2"), unwrapList(t.getReadAreaExclude()));
        assertEquals("t3", unwrap(t.getLabel()));
        assertEquals(null, unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_only_start() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_read_write_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION READ ONLY READ WRITE"));
        assertEquals(ErrorKind.CONFLICT_READ_WRITE_MODE_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_write_preserve_invalid() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION WRITE PRESERVE 100"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_write_preserve_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION WRITE PRESERVE a WRITE PRESERVE b"));
        assertEquals(ErrorKind.DUPLICATE_WRITE_PRESERVE_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_read_area_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION READ AREA INCLUDE a READ AREA EXCLUDE b"));
        assertEquals(ErrorKind.DUPLICATE_READ_AREA_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_read_area_include_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION READ AREA INCLUDE a INCLUDE b"));
        assertEquals(ErrorKind.DUPLICATE_READ_AREA_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_read_area_exclude_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION READ AREA EXCLUDE a EXCLUDE b"));
        assertEquals(ErrorKind.DUPLICATE_READ_AREA_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_read_no_include_exclude() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION READ AREA"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_execute_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION EXECUTE PRIOR EXECUTE EXCLUDING"));
        assertEquals(ErrorKind.CONFLICT_EXCLUSIVE_MODE_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_as_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION AS a AS b"));
        assertEquals(ErrorKind.DUPLICATE_TRANSACTION_LABEL_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_with_conflict() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION WITH () WITH ()"));
        assertEquals(ErrorKind.DUPLICATE_TRANSACTION_PROPERTIES_OPTION, e.getErrorKind());
    }

    @Test
    void start_transaction_with_conflict_key() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION WITH a, a"));
        assertEquals(ErrorKind.CONFLICT_PROPERTIES_KEY, e.getErrorKind());
    }

    @Test
    void start_transaction_with_invalid_key() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION WITH 1"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_with_missing_value() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION WITH a="));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_with_sign_missing_number() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION WITH a=+"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_unknown_option() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION INVALID"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_begin_unknown_option() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("BEGIN INVALID"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void commit() throws Exception {
        Statement s = analyze("COMMIT");
        assertEquals(Statement.Kind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(null, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_accepted() throws Exception {
        Statement s = analyze("COMMIT WAIT FOR ACCEPTED");
        assertEquals(Statement.Kind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(CommitStatus.ACCEPTED, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_available() throws Exception {
        Statement s = analyze("COMMIT WAIT FOR Available");
        assertEquals(Statement.Kind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(CommitStatus.AVAILABLE, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_stored() throws Exception {
        Statement s = analyze("COMMIT WAIT FOR stored");
        assertEquals(Statement.Kind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(CommitStatus.STORED, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_propagated() throws Exception {
        Statement s = analyze("COMMIT WAIT PROPAGATED");
        assertEquals(Statement.Kind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(CommitStatus.PROPAGATED, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_unknown() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("COMMIT WAIT FOR UNKNOWN"));
        assertEquals(ErrorKind.UNKNOWN_COMMIT_STATUS, e.getErrorKind());
    }

    @Test
    void commit_unknown_option() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("COMMIT UNKNOWN"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void rollback() throws Exception {
        Statement s = analyze("ROLLBACK");
        assertEquals(Statement.Kind.ROLLBACK, s.getKind());
    }

    @Test
    void rollback_unknown_option() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("ROLLBACK FOR ME"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void call_empty_arguments() throws Exception {
        Statement s = analyze("CALL f()");
        assertEquals(Statement.Kind.CALL, s.getKind());
        var t = (CallStatement) s;
        assertEquals("f", t.getProcedureName().getValue());
        assertEquals(List.of(), unwrapList(t.getProcedureArguments()));
    }

    @Test
    void call_argument() throws Exception {
        Statement s = analyze("CALL f(1)");
        assertEquals(Statement.Kind.CALL, s.getKind());
        var t = (CallStatement) s;
        assertEquals("f", t.getProcedureName().getValue());
        assertEquals(List.of(Value.of(1)), unwrapList(t.getProcedureArguments()));
    }

    @Test
    void call_multiple_arguments() throws Exception {
        Statement s = analyze("CALL f(1, 2, 3)");
        assertEquals(Statement.Kind.CALL, s.getKind());
        var t = (CallStatement) s;
        assertEquals("f", t.getProcedureName().getValue());
        assertEquals(List.of(Value.of(1), Value.of(2), Value.of(3)), unwrapList(t.getProcedureArguments()));
    }

    @Test
    void call_delimited_name() throws Exception {
        Statement s = analyze("CALL \"f\"()");
        assertEquals(Statement.Kind.CALL, s.getKind());
        var t = (CallStatement) s;
        assertEquals("\"f\"", t.getProcedureName().getValue());
        assertEquals(List.of(), unwrapList(t.getProcedureArguments()));
    }

    @Test
    void call_qualified_name() throws Exception {
        Statement s = analyze("CALL f.g.h()");
        assertEquals(Statement.Kind.CALL, s.getKind());
        var t = (CallStatement) s;
        assertEquals("f.g.h", t.getProcedureName().getValue());
        assertEquals(List.of(), unwrapList(t.getProcedureArguments()));
    }

    @Test
    void call_qualified_identifier() throws Exception {
        Statement s = analyze("CALL f(a.b.c)");
        assertEquals(Statement.Kind.CALL, s.getKind());
        var t = (CallStatement) s;
        assertEquals("f", t.getProcedureName().getValue());
        assertEquals(List.of(Value.of("a.b.c")), unwrapList(t.getProcedureArguments()));
    }

    @Test
    void call_fallback() throws Exception {
        Statement s = analyze("CALL f(1 + 2)");
        assertEquals(Statement.Kind.GENERIC, s.getKind());
        assertEquals("CALL f(1 + 2)", s.getText());
    }

    @Test
    void special() throws Exception {
        Statement s = analyze("\\EXIT");
        assertEquals(Statement.Kind.SPECIAL, s.getKind());
        var t = (SpecialStatement) s;
        assertEquals("EXIT", t.getCommandName().getValue());
    }

    @Test
    void string_escape() throws Exception {
        Statement s = analyze("BEGIN TRANSACTION AS '\\\\\\'\\n\\r\\t'");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals("\\'\n\r\t", unwrap(t.getLabel()));
    }

    @Test
    void identifier_or_string_invalid() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION AS 100"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void start_transaction_with_identifier() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=testing");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of("testing")), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_character_numeric() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=3.14");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of(new BigDecimal("3.14"))), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_character_plus_numeric() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=+3.14");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of(new BigDecimal("3.14"))), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_character_minus_numeric() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=-3.14");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of(new BigDecimal("-3.14"))), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_character_string() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a='OK'");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of("OK")), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_true() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=true");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of(true)), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_false() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=false");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of(false)), unwrapMap(t.getProperties()));
    }

    @Test
    void start_transaction_with_null() throws Exception {
        Statement s = analyze("START TRANSACTION WITH a=null");
        assertEquals(Statement.Kind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
        assertEquals(Map.of("a", Value.of()), unwrapMap(t.getProperties()));
    }

    private static Statement analyze(String text) throws IOException, ParseException {
        try (var scanner = new SqlScanner(new StringReader(text))) {
            return SegmentAnalyzer.analyze(scanner.next());
        }
    }

    private static <T> T unwrap(Optional<Regioned<T>> wrapped) {
        if (wrapped.isEmpty()) {
            return null;
        }
        return wrapped.get().getValue();
    }

    private static <T> List<T> unwrapList(Optional<List<Regioned<T>>> wrapped) {
        if (wrapped.isEmpty()) {
            return null;
        }
        return wrapped.get().stream()
                .map(Regioned::getValue)
                .collect(Collectors.toList());
    }

    private static <T> List<T> unwrapList(List<Regioned<T>> wrapped) {
        return unwrapList(Optional.of(wrapped));
    }

    private static <K> Map<K, Value> unwrapMap(Optional<Map<Regioned<K>, Optional<Regioned<Value>>>> wrapped) {
        if (wrapped.isEmpty()) {
            return null;
        }
        return wrapped.get().entrySet().stream()
                .map(it -> Map.entry(it.getKey().getValue(), it.getValue().map(Regioned::getValue).orElse(Value.of())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
