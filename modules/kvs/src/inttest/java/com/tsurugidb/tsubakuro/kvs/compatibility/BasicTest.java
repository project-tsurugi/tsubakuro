package com.tsurugidb.tsubakuro.kvs.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;
import com.tsurugidb.tsubakuro.sql.SqlClient;

class BasicTest extends TestBase {

    private static final String TABLE_NAME = "table" + BasicTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    private void createTable() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    private static void checkSQLrecord(SqlClient sql, long key, long value) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%d", TABLE_NAME, KEY_NAME, key);
            var vals = new ArrayList<Long>(2);
            try (var rs = tx.executeQuery(st).await()) {
                while (rs.nextRow()) {
                    while (rs.nextColumn()) {
                        vals.add(rs.fetchInt8Value());
                    }
                }
            }
            tx.commit().await();
            assertEquals(2, vals.size());
            assertEquals(key, vals.get(0));
            assertEquals(value, vals.get(1));
        }
    }

    private static void checkSQLnotFound(SqlClient sql, long key) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%d", TABLE_NAME, KEY_NAME, key);
            int n = 0;
            try (var rs = tx.executeQuery(st).await()) {
                while (rs.nextRow()) {
                    while (rs.nextColumn()) {
                        n++;
                    }
                }
            }
            tx.commit().await();
            assertEquals(0, n);
        }
    }

    private static void checkSQLisNull(SqlClient sql, long key) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%d", TABLE_NAME, KEY_NAME, key);
            int n = 0;
            try (var rs = tx.executeQuery(st).await()) {
                while (rs.nextRow()) {
                    while (rs.nextColumn()) {
                        if (n != 1) {
                            assertEquals(key, rs.fetchInt8Value());
                        } else {
                            assertEquals(true, rs.isNull());
                        }
                        n++;
                    }
                }
            }
            tx.commit().await();
            assertEquals(2, n);
        }
    }

    private static void checkKVSrecord(KvsClient kvs, long key, long value) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, TABLE_NAME, buffer).await();
            kvs.commit(tx).await();
            assertEquals(1, get.size());
            var record = get.asRecord();
            assertEquals(key, record.getLong(KEY_NAME));
            assertEquals(value, record.getLong(VALUE_NAME));
        }
    }

    private static void checkKVSnotFound(KvsClient kvs, long key) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, TABLE_NAME, buffer).await();
            kvs.commit(tx).await();
            assertEquals(0, get.size());
        }
    }

    private static void checkKVSisNull(KvsClient kvs, long key) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, TABLE_NAME, buffer).await();
            kvs.commit(tx).await();
            assertEquals(1, get.size());
            var record = get.asRecord();
            assertEquals(key, record.getLong(KEY_NAME));
            assertEquals(true, record.isNull(VALUE_NAME));
        }
    }

    private static void checkRecord(SqlClient sql, KvsClient kvs, long key, long value) throws Exception {
        checkSQLrecord(sql, key, value);
        checkKVSrecord(kvs, key, value);
    }

    private static void checkNotFound(SqlClient sql, KvsClient kvs, long key) throws Exception {
        checkSQLnotFound(sql, key);
        checkKVSnotFound(kvs, key);
    }

    private static void checkIsNull(SqlClient sql, KvsClient kvs, long key) throws Exception {
        checkSQLisNull(sql, key);
        checkKVSisNull(kvs, key);
    }

    @Test
    public void basicPutGetRemove() throws Exception {
        createTable();
        final long key1 = 1L;
        final long value1 = 100L;
        final long key2 = 2L;
        final long value2 = 200L;
        try (var session = getNewSession(); var sql = SqlClient.attach(session); var kvs = KvsClient.attach(session)) {
            // SQL INSERT
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("INSERT INTO %s (%s, %s) VALUES(%d, %d)", TABLE_NAME, KEY_NAME, VALUE_NAME, key1,
                        value1);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value1);

            // KVS PUT (OVERWRITE)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value2);

            // KVS PUT (INSERT)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2);
                buffer.add(VALUE_NAME, value1);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value2); // not changed
            checkRecord(sql, kvs, key2, value1); // newly inserted

            // SQL UPDATE
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("UPDATE %s SET %s=%d WHERE %s=%d", TABLE_NAME, VALUE_NAME, value2, KEY_NAME,
                        key2);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value2); // not changed
            checkRecord(sql, kvs, key2, value2); // value updated

            // SQL REMOVE
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("DELETE FROM %s WHERE %s=%d", TABLE_NAME, KEY_NAME, key1);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkNotFound(sql, kvs, key1); // deleted
            checkRecord(sql, kvs, key2, value2); // not changed

            // KVS REMOVE
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2);
                var remove = kvs.remove(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, remove.size());
            }
            checkNotFound(sql, kvs, key1); // already deleted
            checkNotFound(sql, kvs, key2); // deleted
        }
    }

    @Test
    public void basicNull() throws Exception {
        createTable();
        final long key1 = 1L;
        final long value1 = 100L;
        final long key2 = 2L;
        final long value2 = 200L;
        try (var session = getNewSession(); var sql = SqlClient.attach(session); var kvs = KvsClient.attach(session)) {
            // SQL INSERT (value is null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("INSERT INTO %s (%s) VALUES(%d)", TABLE_NAME, KEY_NAME, key1);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkIsNull(sql, kvs, key1);

            // KVS INSERT (value is null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2);
                buffer.addNull(VALUE_NAME);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkIsNull(sql, kvs, key1);
            checkIsNull(sql, kvs, key2);

            // SQL UPDATE (make value non-null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("UPDATE %s SET %s=%d WHERE %s=%d", TABLE_NAME, VALUE_NAME, value1, KEY_NAME,
                        key1);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value1); // null -> value1
            checkIsNull(sql, kvs, key2);

            // KVS UPDATE (make value non-null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2);
                buffer.add(VALUE_NAME, value2);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value1);
            checkRecord(sql, kvs, key2, value2); // null -> value2

            // SQL UPDATE (make value null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("UPDATE %s SET %s=NULL WHERE %s=%d", TABLE_NAME, VALUE_NAME, KEY_NAME, key1);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkIsNull(sql, kvs, key1); // value1 -> null
            checkRecord(sql, kvs, key2, value2);

            // KVS UPDATE (make value null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2);
                buffer.addNull(VALUE_NAME);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkIsNull(sql, kvs, key1);
            checkIsNull(sql, kvs, key2); // value2 -> null

            // SQL DELETE (value is null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("DELETE FROM %s WHERE %s=%d", TABLE_NAME, KEY_NAME, key1);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkNotFound(sql, kvs, key1); // deleted
            checkIsNull(sql, kvs, key2);

            // KVS REMOVE (value is null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2);
                var remove = kvs.remove(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, remove.size());
            }
            checkNotFound(sql, kvs, key1);
            checkNotFound(sql, kvs, key2); // deleted
        }
    }
}
