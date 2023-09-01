package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.RemoveType;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;
import com.tsurugidb.tsubakuro.sql.SqlClient;

public class BasicPutGetRemoveTest extends TestBase {

    private static final String TABLE_NAME = "table" + BasicPutGetRemoveTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public BasicPutGetRemoveTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    private void checkRecord(Record record, long key1, long value1) throws Exception {
        final int idxKey = 0; // TODO maybe change
        final int idxValue = 1;
        assertEquals(record.getName(idxKey), KEY_NAME);
        assertEquals(record.getName(idxValue), VALUE_NAME);
        assertEquals(record.getValue(idxKey), key1);
        assertEquals(record.getValue(idxValue), value1);
        assertEquals(record.getLong(KEY_NAME), key1);
        assertEquals(record.getLong(VALUE_NAME), value1);
    }

    @Test
    public void basicPutGetRemove() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
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
                checkRecord(get.asRecord(), key1, value1);
                assertEquals(get.asList().size(), 1);
                checkRecord(get.asList().get(0), key1, value1);
                assertEquals(get.isEmpty(), false);
                assertEquals(get.isSingle(), true);
                assertEquals(get.asOptional().isPresent(), true);
                checkRecord(get.asOptional().get(), key1, value1);
            }
            try (var tx = kvs.beginTransaction().await()) {
                var remove = kvs.remove(tx, TABLE_NAME, keyBuffer).await();
                kvs.commit(tx).await();
                assertEquals(remove.size(), 1);
            }
        }
    }

    @Test
    public void putTypes() throws Exception {
        final long key1 = 2L;
        final long key2 = 3L;
        final long key3 = 4L;
        final long value1 = 200L;
        final long value2 = 201L;
        final long value3 = 202L;
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            // {}; add initial (key1, value1)
            try (var tx = kvs.beginTransaction().await()) {
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
                var put = kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            // {(key1, value1)}
            // check PutType.IF_PRESENT fail; key2 doesn't exist
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key2);
                buffer.add(VALUE_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer, PutType.IF_PRESENT).await();
                kvs.rollback(tx).await();
                assertEquals(put.size(), 0);
            }
            // {(key1, value1)}
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key2);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 0);
            }
            // {(key1, value1)}
            // check PutType.IF_PRESENT success; key1 exists, update to (key1, value2)
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer, PutType.IF_PRESENT).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            // {(key1, value2)}
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                checkRecord(get.asRecord(), key1, value2);
            }
            // {(key1, value2)}
            // check PutType.IF_ABSENT fail; key1 exists
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value3);
                var put = kvs.put(tx, TABLE_NAME, buffer, PutType.IF_ABSENT).await();
                assertEquals(put.size(), 0);
                kvs.rollback(tx).await();
            }
            // {(key1, value2)}
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                checkRecord(get.asRecord(), key1, value2);
            }
            // {(key1, value2)}
            // check PutType.IF_ABSENT success; key3 doesn't exist, insert (key3, value3)
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key3);
                buffer.add(VALUE_NAME, value3);
                var put = kvs.put(tx, TABLE_NAME, buffer, PutType.IF_ABSENT).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            // {(key1, value2), (key3, value3)}
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key3);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size());
                checkRecord(get.asRecord(), key3, value3);
            }
            assertEquals(PutType.OVERWRITE, PutType.DEFAULT_BEHAVIOR);
            // {(key1, value2), (key3, value3)}; without PutType means OVERWRITE
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value3);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            // {(key1, value3), (key3, value3)}
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size());
                checkRecord(get.asRecord(), key1, value3);
            }
            // {(key1, value2), (key3, value3)}; without PutType means OVERWRITE
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key2);
                buffer.add(VALUE_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            // {(key1, value3), (key3, value3), (key2, value2)}
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key2);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size());
                checkRecord(get.asRecord(), key2, value2);
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size());
                checkRecord(get.asRecord(), key1, value3);
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key3);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size());
                checkRecord(get.asRecord(), key3, value3);
            }
        }
    }

    @Test
    public void removeTypes() throws Exception {
        final long key1 = 2L;
        final long key2 = 3L;
        final long value = 100L;
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value);
                var put = kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            // {(key1, 100)}; remove non-exist key
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key2);
                var rem = kvs.remove(tx, TABLE_NAME, buffer, RemoveType.COUNTING).await();
                kvs.commit(tx).await();
                assertEquals(0, rem.size()); // means no record removed
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size()); // (key1, value) exists
            }
            // {(key1, 100)}; remove exists key
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var rem = kvs.remove(tx, TABLE_NAME, buffer, RemoveType.COUNTING).await();
                kvs.commit(tx).await();
                assertEquals(1, rem.size()); // means 1 record removed
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(0, get.size()); // (key1, value) doesn't exist
            }
            // insert again
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value);
                var put = kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            // {(key1, 100)}; remove non-exist key
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key2);
                var rem = kvs.remove(tx, TABLE_NAME, buffer, RemoveType.INSTANT).await();
                kvs.commit(tx).await();
                assertEquals(1, rem.size()); // means 1 remove operation called
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size()); // (key1, value) exists
            }
            // {(key1, 100)}; remove exists key
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var rem = kvs.remove(tx, TABLE_NAME, buffer, RemoveType.INSTANT).await();
                kvs.commit(tx).await();
                assertEquals(1, rem.size()); // means 1 remove operation called
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(0, get.size()); // (key1, value) doesn't exist
            }
        }
    }
}
