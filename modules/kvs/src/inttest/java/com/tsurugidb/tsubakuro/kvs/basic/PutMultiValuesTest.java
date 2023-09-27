package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class PutMultiValuesTest extends TestBase {

    private static final String TABLE_NAME = "table" + PutMultiValuesTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE1_NAME = "v1";
    private static final String VALUE2_NAME = "v2";

    public PutMultiValuesTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT, %s BIGINT", KEY_NAME, VALUE1_NAME,
                VALUE2_NAME);
        createTable(TABLE_NAME, schema);
    }

    private static void checkRecord(Record record, long key1, long value1, long value2) throws Exception {
        final int idxKey = 0; // TODO maybe change
        final int idxValue1 = 1;
        final int idxValue2 = 2;
        assertEquals(record.getName(idxKey), KEY_NAME);
        assertEquals(record.getName(idxValue1), VALUE1_NAME);
        assertEquals(record.getName(idxValue2), VALUE2_NAME);
        assertEquals(record.getValue(idxKey), key1);
        assertEquals(record.getValue(idxValue1), value1);
        assertEquals(record.getValue(idxValue2), value2);
        assertEquals(record.getLong(KEY_NAME), key1);
        assertEquals(record.getLong(VALUE1_NAME), value1);
        assertEquals(record.getLong(VALUE2_NAME), value2);
    }

    @Test
    public void basic() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        final long value2 = 101L;
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value1);
                buffer.add(VALUE2_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            RecordBuffer keyBuffer = new RecordBuffer();
            keyBuffer.add(KEY_NAME, key1);
            try (var tx = kvs.beginTransaction().await()) {
                var get = kvs.get(tx, TABLE_NAME, keyBuffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                checkRecord(get.asRecord(), key1, value1, value2);
                assertEquals(get.asList().size(), 1);
                checkRecord(get.asList().get(0), key1, value1, value2);
                assertEquals(get.isEmpty(), false);
                assertEquals(get.isSingle(), true);
                assertEquals(get.asOptional().isPresent(), true);
                checkRecord(get.asOptional().get(), key1, value1, value2);
            }
            try (var tx = kvs.beginTransaction().await()) {
                var remove = kvs.remove(tx, TABLE_NAME, keyBuffer).await();
                kvs.commit(tx).await();
                assertEquals(remove.size(), 1);
            }
        }
    }

    @Test
    public void columnOrder() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        final long value2 = 101L;
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(VALUE1_NAME, value2);
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE2_NAME, value1);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            RecordBuffer keyBuffer = new RecordBuffer();
            keyBuffer.add(KEY_NAME, key1);
            try (var tx = kvs.beginTransaction().await()) {
                var get = kvs.get(tx, TABLE_NAME, keyBuffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                checkRecord(get.asRecord(), key1, value2, value1);
            }
            //
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(VALUE2_NAME, value2);
                buffer.add(VALUE1_NAME, value1);
                buffer.add(KEY_NAME, key1);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            try (var tx = kvs.beginTransaction().await()) {
                var get = kvs.get(tx, TABLE_NAME, keyBuffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                checkRecord(get.asRecord(), key1, value1, value2);
            }
            //
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(VALUE2_NAME, value1);
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            try (var tx = kvs.beginTransaction().await()) {
                var get = kvs.get(tx, TABLE_NAME, keyBuffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                checkRecord(get.asRecord(), key1, value2, value1);
            }
        }
    }

    @Test
    public void invalidRequests() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        final long value2 = 101L;
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value1);
                buffer.add(VALUE2_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            // INCOMPLETE_COLUMNS
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer).await());
                assertEquals(KvsServiceCode.INCOMPLETE_COLUMNS, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer).await());
                assertEquals(KvsServiceCode.INCOMPLETE_COLUMNS, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // INVALID_ARGUMENT
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                // empty
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value1);
                buffer.add(VALUE2_NAME, value2);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value1);
                buffer.add(VALUE2_NAME, value2);
                buffer.add(VALUE1_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value1);
                buffer.add(VALUE2_NAME, value2);
                buffer.add(VALUE1_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer).await());
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // COLUMN_NOT_FOUND
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE1_NAME, value1);
                buffer.add(VALUE2_NAME, value2);
                buffer.add("hoge", value2);
                KvsServiceException ex = assertThrows(KvsServiceException.class,
                        () -> kvs.put(tx, TABLE_NAME, buffer).await());
                assertEquals(KvsServiceCode.COLUMN_NOT_FOUND, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
        }
    }
}
