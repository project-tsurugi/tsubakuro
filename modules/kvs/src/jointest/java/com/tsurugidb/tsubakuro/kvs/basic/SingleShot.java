package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;
import com.tsurugidb.tsubakuro.sql.SqlClient;

public class SingleShot extends TestBase {

    private static final String TABLE_NAME = "table" + SingleShot.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    private void createTable(String tableName) throws Exception {
        try (var session = getNewSession(); var client = SqlClient.attach(session)) {
            try {
                try (var tx = client.createTransaction().await()) {
                    String sql = String.format("DROP TABLE %s", tableName);
                    tx.executeStatement(sql).await();
                    tx.commit().await();
                }
            } catch (Exception e) {
                var msg = e.getMessage();
                if (!msg.contains("table_not_found") && !msg.contains("not found")) {
                    e.printStackTrace();
                }
            }
            try (var tx = client.createTransaction().await()) {
                String sql = String.format("CREATE TABLE %s (%s BIGINT PRIMARY KEY, %s BIGINT)", tableName, KEY_NAME,
                        VALUE_NAME);
                tx.executeStatement(sql).await();
                tx.commit().await();
            }
        }
    }

    private void checkRecord(Record record, long key1, long value1) throws Exception {
        assertEquals(record.getName(0), KEY_NAME);
        assertEquals(record.getName(1), VALUE_NAME);
        assertEquals(record.getValue(0), key1);
        assertEquals(record.getValue(1), value1);
        assertEquals(record.getLong(KEY_NAME), key1);
        assertEquals(record.getLong(VALUE_NAME), value1);
    }

    @Test
    public void basicPutGetRemove() throws Exception {
        createTable(TABLE_NAME);
        final long key1 = 1L;
        final long value1 = 100L;
        try (var session = getNewSession(); var kvs = KvsClient.attach(session);
            var tx = kvs.beginTransaction().await()) {
            {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                assertEquals(put.size(), 1);
            }
            RecordBuffer keyBuffer = new RecordBuffer();
            keyBuffer.add(KEY_NAME, key1);
            {
                var get = kvs.get(tx, TABLE_NAME, keyBuffer).await();
                assertEquals(get.size(), 1);
                checkRecord(get.asRecord(), key1, value1);
                assertEquals(get.asList().size(), 1);
                checkRecord(get.asList().get(0), key1, value1);
                assertEquals(get.isEmpty(), false);
                assertEquals(get.isSingle(), true);
                assertEquals(get.asOptional().isPresent(), true);
                checkRecord(get.asOptional().get(), key1, value1);
            }
            {
                var remove = kvs.remove(tx, TABLE_NAME, keyBuffer).await();
                assertEquals(remove.size(), 1);
            }
        }
    }
}
