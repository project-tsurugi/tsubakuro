package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class SessionClientTest extends TestBase {

    private static final String TABLE_NAME = "table" + SessionClientTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public SessionClientTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    @Test
    public void reuseSession() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        try (var session = getNewSession()) {
            try (var kvs = KvsClient.attach(session)) {
                try (var tx = kvs.beginTransaction().await()) {
                    RecordBuffer buffer = new RecordBuffer();
                    buffer.add(KEY_NAME, key1);
                    buffer.add(VALUE_NAME, value1);
                    var put = kvs.put(tx, TABLE_NAME, buffer).await();
                    kvs.commit(tx).await();
                    assertEquals(1, put.size());
                }
                try (var tx = kvs.beginTransaction().await()) {
                    RecordBuffer buffer = new RecordBuffer();
                    buffer.add(KEY_NAME, key1);
                    buffer.add(VALUE_NAME, value1);
                    var put = kvs.put(tx, TABLE_NAME, buffer).await();
                    kvs.commit(tx).await();
                    assertEquals(1, put.size());
                }
            }
            // kvs client closed, but the session doesn't closed
            // reuse the session again
            try (var kvs = KvsClient.attach(session); var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
        }
    }

    @Test
    public void recloses() throws Exception {
        try (var session = getNewSession()) {
            try (var kvs = KvsClient.attach(session)) {
                try (var tx = kvs.beginTransaction().await()) {
                    tx.close();
                }
                kvs.close();
            }
            session.close();
        }
    }

    @Test
    public void missingTxClose() throws Exception {
        final long key1 = 1L;
        final long value1 = 100L;
        final long value2 = 101L;
        try (var session = getNewSession()) {
            try (var kvs = KvsClient.attach(session)) {
                try (var tx = kvs.beginTransaction().await()) {
                    RecordBuffer buffer = new RecordBuffer();
                    buffer.add(KEY_NAME, key1);
                    buffer.add(VALUE_NAME, value1);
                    var put = kvs.put(tx, TABLE_NAME, buffer).await();
                    kvs.commit(tx).await();
                    assertEquals(1, put.size());
                }
                {
                    // not use try-catch-resource
                    var tx2 = kvs.beginTransaction().await();
                    RecordBuffer buffer = new RecordBuffer();
                    buffer.add(KEY_NAME, key1);
                    buffer.add(VALUE_NAME, value2);
                    var put = kvs.put(tx2, TABLE_NAME, buffer).await();
                    assertEquals(1, put.size());
                    // missing commit, missing tx.close.
                }
                // kvs.close() calls missing tx2.close(), it calls tx2.rollback()
                // update to value2 is aborted
            }
            try (var kvs = KvsClient.attach(session); var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size());
                var record = get.asRecord();
                assertEquals(key1, record.getLong(KEY_NAME));
                assertEquals(value1, record.getLong(VALUE_NAME));
            }
        }
    }
}
