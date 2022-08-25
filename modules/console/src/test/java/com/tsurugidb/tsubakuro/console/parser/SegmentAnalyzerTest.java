package com.tsurugidb.tsubakuro.console.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.console.model.Regioned;
import com.tsurugidb.tsubakuro.console.model.SpecialStatement;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement;
import com.tsurugidb.tsubakuro.console.model.Statement;
import com.tsurugidb.tsubakuro.console.model.StatementKind;
import com.tsurugidb.tsubakuro.console.model.CommitStatement;
import com.tsurugidb.tsubakuro.console.model.CommitStatement.CommitStatus;
import com.tsurugidb.tsubakuro.console.model.ErroneousStatement.ErrorKind;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ExclusiveMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ReadWriteMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.TransactionMode;

class SegmentAnalyzerTest {

    @Test
    void simple() throws Exception {
        Statement s = analyze("SELECT * FROM TBL");
        assertEquals(StatementKind.GENERIC, s.getKind());
        assertEquals("SELECT * FROM TBL", s.getText());
    }

    @Test
    void empty_eof() throws Exception {
        Statement s = analyze("");
        assertEquals(StatementKind.EMPTY, s.getKind());
        assertEquals("", s.getText());
    }

    @Test
    void empty_semicolon() throws Exception {
        Statement s = analyze(";");
        assertEquals(StatementKind.EMPTY, s.getKind());
        assertEquals("", s.getText());
    }

    @Test
    void start_transaction() throws Exception {
        Statement s = analyze("START TRANSACTION");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals("START TRANSACTION", s.getText());
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void begin_transaction() throws Exception {
        Statement s = analyze("BEGIN TRANSACTION");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals("BEGIN TRANSACTION", s.getText());
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_long_transaction() throws Exception {
        Statement s = analyze("START LONG TRANSACTION");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(TransactionMode.LONG, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_read_only() throws Exception {
        Statement s = analyze("START TRANSACTION READ ONLY");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(ReadWriteMode.READ_ONLY, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_read_only_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION READ ONLY DEFERRABLE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(ReadWriteMode.READ_ONLY_DEFERRABLE, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_read_write() throws Exception {
        Statement s = analyze("START TRANSACTION READ WRITE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(ReadWriteMode.READ_WRITE, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_empty() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE ()");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of(), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_identifier() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_delimited() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE \"Hello, world!\"");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("\"Hello, world!\""), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_identifier_chain() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a.b.c");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a.b.c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_glob() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a.b.*");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a.b.*"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_string() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE 'a.b.c'");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a.b.c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_multiple() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE a, b, c");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a", "b", "c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_write_preserve_multiple_paren() throws Exception {
        Statement s = analyze("START TRANSACTION WRITE PRESERVE (a, b, c)");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("a", "b", "c"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_read_area_include() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA INCLUDE a");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_read_area_exclude() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA EXCLUDE a");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_read_area_include_exclude() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA INCLUDE a EXCLUDE b");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("b"), unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_read_area_exclude_include() throws Exception {
        Statement s = analyze("START TRANSACTION READ AREA EXCLUDE a INCLUDE b");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(List.of("b"), unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("a"), unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_prior() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE PRIOR");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.PRIOR_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_prior_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE PRIOR DEFERRABLE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.PRIOR_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_prior_immediate() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE PRIOR IMMEDIATE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.PRIOR_IMMEDIATE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_excluding() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDING");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_excluding_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDING DEFERRABLE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_excluding_immediate() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDING IMMEDIATE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_IMMEDIATE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_exclude() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_exclude_deferrable() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDE DEFERRABLE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_DEFERRABLE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_execute_exclude_immediate() throws Exception {
        Statement s = analyze("START TRANSACTION EXECUTE EXCLUDE IMMEDIATE");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(ExclusiveMode.EXCLUDING_IMMEDIATE, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals(null, unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_as() throws Exception {
        Statement s = analyze("START TRANSACTION AS tx1");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals("tx1", unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_as_string() throws Exception {
        Statement s = analyze("START TRANSACTION AS 'Hello, world!'");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(null, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(null, unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(null, unwrapList(t.getReadAreaExclude()));
        assertEquals("Hello, world!", unwrap(t.getLabel()));
    }

    @Test
    void start_transaction_LTX() throws Exception {
        Statement s = analyze("START LONG TRANSACTION WRITE PRESERVE t1 READ AREA EXCLUDE t2 AS t3;");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals(TransactionMode.LONG, unwrap(t.getTransactionMode()));
        assertEquals(null, unwrap(t.getReadWriteMode()));
        assertEquals(null, unwrap(t.getExclusiveMode()));
        assertEquals(List.of("t1"), unwrapList(t.getWritePreserve()));
        assertEquals(null, unwrapList(t.getReadAreaInclude()));
        assertEquals(List.of("t2"), unwrapList(t.getReadAreaExclude()));
        assertEquals("t3", unwrap(t.getLabel()));
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
    void start_transaction_unknown_option() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION INVALID"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void commit() throws Exception {
        Statement s = analyze("COMMIT");
        assertEquals(StatementKind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(null, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_accepted() throws Exception {
        Statement s = analyze("COMMIT WAIT FOR ACCEPTED");
        assertEquals(StatementKind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(CommitStatus.ACCEPTED, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_available() throws Exception {
        Statement s = analyze("COMMIT WAIT FOR Available");
        assertEquals(StatementKind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(CommitStatus.AVAILABLE, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_stored() throws Exception {
        Statement s = analyze("COMMIT WAIT FOR stored");
        assertEquals(StatementKind.COMMIT, s.getKind());
        var t = (CommitStatement) s;
        assertEquals(CommitStatus.STORED, unwrap(t.getCommitStatus()));
    }

    @Test
    void commit_wait_for_propagated() throws Exception {
        Statement s = analyze("COMMIT WAIT PROPAGATED");
        assertEquals(StatementKind.COMMIT, s.getKind());
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
        assertEquals(StatementKind.ROLLBACK, s.getKind());
    }

    @Test
    void rollback_unknown_option() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("ROLLBACK FOR ME"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
    }

    @Test
    void special() throws Exception {
        Statement s = analyze("\\EXIT");
        assertEquals(StatementKind.SPECIAL, s.getKind());
        var t = (SpecialStatement) s;
        assertEquals("EXIT", t.getCommandName().getValue());
    }

    @Test
    void string_escape() throws Exception {
        Statement s = analyze("BEGIn TRANSACTION AS '\\\\\\'\\n\\r\\t'");
        assertEquals(StatementKind.START_TRANSACTION, s.getKind());
        var t = (StartTransactionStatement) s;
        assertEquals("\\'\n\r\t", unwrap(t.getLabel()));
    }

    @Test
    void identifier_or_string_invalid() throws Exception {
        ParseException e = assertThrows(ParseException.class, () -> analyze("START TRANSACTION AS 100"));
        assertEquals(ErrorKind.UNEXPECTED_TOKEN, e.getErrorKind());
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
}
