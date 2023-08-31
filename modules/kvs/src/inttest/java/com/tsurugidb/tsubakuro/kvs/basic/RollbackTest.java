package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedList;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

public class RollbackTest extends TestBase {

    private static final String TABLE_NAME = "table" + RollbackTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public RollbackTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    @Test
    public void rollbackUpdateAndRemove() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        final long value2 = 200L;
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            // initial insert
            try (var tx = kvs.beginTransaction().await()) {
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(put.size(), 1);
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                var record = get.asRecord();
                assertEquals(record.getLong(KEY_NAME), key1);
                assertEquals(record.getLong(VALUE_NAME), value1);
            }
            // update and abort
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.rollback(tx).await();
                assertEquals(put.size(), 1);
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                var record = get.asRecord();
                assertEquals(record.getLong(KEY_NAME), key1);
                assertEquals(record.getLong(VALUE_NAME), value1); // not value2
            }
            // remove and abort
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value2);
                var remove = kvs.remove(tx, TABLE_NAME, buffer).await();
                kvs.rollback(tx).await();
                assertEquals(remove.size(), 1);
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(get.size(), 1);
                var record = get.asRecord();
                assertEquals(record.getLong(KEY_NAME), key1); // found key1, not removed
                assertEquals(record.getLong(VALUE_NAME), value1); // not value2
            }
        }
    }

}
