package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedList;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

public class PutTest extends TestBase {

    private static final String TABLE_NAME = "table" + PutTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public PutTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    @Test
    public void invalidRequests() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            // COLUMN_TYPE_MISMATCH
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, (int)key1);
                buffer.add(VALUE_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.COLUMN_TYPE_MISMATCH, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, (int)value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.COLUMN_TYPE_MISMATCH, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, (int)key1);
                buffer.add(VALUE_NAME, (int)value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.COLUMN_TYPE_MISMATCH, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, "aaa");
                buffer.add(VALUE_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.COLUMN_TYPE_MISMATCH, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // MISMATCH_KEY
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(VALUE_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.MISMATCH_KEY, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // INCOMPLETE_COLUMNS
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.INCOMPLETE_COLUMNS, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // INVALID_ARGUMENT
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                // empty record
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
                buffer.add("v2", value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
        }
    }
}
