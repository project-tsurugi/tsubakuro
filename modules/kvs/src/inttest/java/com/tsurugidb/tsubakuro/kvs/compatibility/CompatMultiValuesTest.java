package com.tsurugidb.tsubakuro.kvs.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.Values;
import com.tsurugidb.tsubakuro.sql.SqlClient;

class CompatMultiValuesTest extends CompatBase {

    private static final String TABLE_NAME = "table" + CompatMultiValuesTest.class.getSimpleName();

    public CompatMultiValuesTest() throws Exception {
        super(TABLE_NAME);
    }

    @Test
    public void basic() throws Exception {
        String typeName = "bigint";
        createTable(TABLE_NAME, schemaV2(typeName));
        final SqlValue key1 = new SqlValue(typeName, Values.of(1L));
        final SqlValue key2 = new SqlValue(typeName, Values.of(2L));
        final SqlValue value1 = new SqlValue(typeName, Values.of(100L));
        final SqlValue value2 = new SqlValue(typeName, Values.of(200L));
        try (var session = getNewSession(); var sql = SqlClient.attach(session); var kvs = KvsClient.attach(session)) {
            // SQL INSERT
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("INSERT INTO %s (%s, %s, %s) VALUES(%s, %s, %s)", TABLE_NAME, KEY_NAME,
                        VALUE_NAME, VALUE2_NAME, key1.str, value1.str, value2.str);
                System.err.println(st);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value1, value2);

            // KVS PUT (OVERWRITE)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1.value);
                buffer.add(VALUE_NAME, value2.value);
                buffer.add(VALUE2_NAME, value1.value);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value2, value1);

            // KVS PUT (INSERT)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2.value);
                buffer.add(VALUE_NAME, value1.value);
                buffer.add(VALUE2_NAME, value2.value);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value2, value1); // not changed
            checkRecord(sql, kvs, key2, value1, value2); // newly inserted

            // SQL UPDATE
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("UPDATE %s SET %s=%s WHERE %s=%s", TABLE_NAME, VALUE_NAME, value2.str, KEY_NAME,
                        key2.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value2, value1); // not changed
            checkRecord(sql, kvs, key2, value2, value2); // value updated

            // SQL REMOVE
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("DELETE FROM %s WHERE %s=%s", TABLE_NAME, KEY_NAME, key1.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkNotFound(sql, kvs, key1); // deleted
            checkRecord(sql, kvs, key2, value2, value2); // not changed

            // KVS REMOVE
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2.value);
                var remove = kvs.remove(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, remove.size());
            }
            checkNotFound(sql, kvs, key1); // already deleted
            checkNotFound(sql, kvs, key2); // deleted
        }
    }

}
