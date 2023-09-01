package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

public class GetTest extends TestBase {

    private static final String TABLE_NAME = "table" + GetTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public GetTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    @Test
    public void invalidRequests() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        RecordBuffer key = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            // COLUMN_TYPE_MISMATCH
            try (var tx = kvs.beginTransaction().await()) {
                key.clear();
                key.add(KEY_NAME, (int)key1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.get(tx, TABLE_NAME, key).await());
                assertEquals(KvsServiceCode.COLUMN_TYPE_MISMATCH, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                key.clear();
                key.add(KEY_NAME, "aaa");
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.get(tx, TABLE_NAME, key).await());
                assertEquals(KvsServiceCode.COLUMN_TYPE_MISMATCH, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // COLUMN_NOT_FOUND
            try (var tx = kvs.beginTransaction().await()) {
                key.clear();
                key.add("hoge", key1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.get(tx, TABLE_NAME, key).await());
                assertEquals(KvsServiceCode.COLUMN_NOT_FOUND, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                key.clear();
                key.add(KEY_NAME, key1);
                key.add("hoge", key1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.get(tx, TABLE_NAME, key).await());
                assertEquals(KvsServiceCode.COLUMN_NOT_FOUND, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // INVALID_ARGUMENT
            try (var tx = kvs.beginTransaction().await()) {
                key.clear();
                // empty key
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.get(tx, TABLE_NAME, key).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                key.clear();
                key.add(KEY_NAME, key1);
                key.add(KEY_NAME, key1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.get(tx, TABLE_NAME, key).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
        }
    }
}
