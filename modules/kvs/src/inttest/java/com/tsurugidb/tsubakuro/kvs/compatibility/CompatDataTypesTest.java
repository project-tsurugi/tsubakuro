package com.tsurugidb.tsubakuro.kvs.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.kvs.proto.KvsData.Value.ValueCase;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.Values;
import com.tsurugidb.tsubakuro.kvs.impl.GetResultImpl;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;

class CompatDataTypesTest extends TestBase {

    private static final String TABLE_NAME = "table" + CompatDataTypesTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    private static final int DECIMAL_SCALE = 2;

    private static String schema(String typeName) {
        return String.format("%s %s PRIMARY KEY, %s %s", KEY_NAME, typeName, VALUE_NAME, typeName);
    }

    static GetResult convert(ResultSet rs) throws Exception {
        var meta = rs.getMetadata();
        var columns = meta.getColumns();
        var records = new ArrayList<KvsData.Record>();
        while (rs.nextRow()) {
            var buffer = new RecordBuffer();
            for (var col : columns) {
                assertEquals(true, rs.nextColumn());
                var name = col.getName();
                switch (col.getAtomType()) {
                case INT4:
                    buffer.add(name, Values.of(rs.fetchInt4Value()));
                    break;
                case INT8:
                    buffer.add(name, Values.of(rs.fetchInt8Value()));
                    break;
                case FLOAT4:
                    buffer.add(name, Values.of(rs.fetchFloat4Value()));
                    break;
                case FLOAT8:
                    buffer.add(name, Values.of(rs.fetchFloat8Value()));
                    break;
                case CHARACTER:
                    buffer.add(name, Values.of(rs.fetchCharacterValue()));
                    break;
                case DECIMAL:
                    buffer.add(name, Values.of(rs.fetchDecimalValue()));
                    break;
                case DATE:
                    buffer.add(name, Values.of(rs.fetchDateValue()));
                    break;
                case TIME_OF_DAY:
                    buffer.add(name, Values.of(rs.fetchTimeOfDayValue()));
                    break;
                case TIME_POINT:
                    buffer.add(name, Values.of(rs.fetchTimePointValue()));
                    break;
                case TYPE_UNSPECIFIED:
                    buffer.addNull(name);
                    break;
                default:
                    break;
                }
            }
            records.add(buffer.toRecord().getEntity());
        }
        return new GetResultImpl(records);
    }

    private static BigDecimal toBigDecimal(KvsData.Value v) {
        return toBigDecimal(v, DECIMAL_SCALE);
    }

    private static void checkValue(KvsData.Value expected, KvsData.Value value) throws Exception {
        if (expected.getValueCase() != ValueCase.DECIMAL_VALUE) {
            assertEquals(expected, value);
        } else {
            var expectedDec = toBigDecimal(expected);
            var valueDec = toBigDecimal(value);
            // NOTE: BigDecimal("12.3") != BigDecimal("12.30")
            System.err.println(expectedDec + "\t" + valueDec);
            assertEquals(expectedDec, valueDec);
            assertEquals(expectedDec.scale(), valueDec.scale());
            assertEquals(expectedDec.toString(), valueDec.toString());
        }
    }

    private static void checkRecord(KvsData.Value key, KvsData.Value value, Record record) throws Exception {
        final int idxKey = 0; // TODO maybe change
        final int idxValue = 1;
        assertEquals(KEY_NAME, record.getName(idxKey));
        assertEquals(VALUE_NAME, record.getName(idxValue));
        checkValue(key, record.getEntity().getValues(idxKey));
        checkValue(value, record.getEntity().getValues(idxValue));
    }

    private static void checkSQLrecord(SqlClient sql, KvsData.Value key, String keystr, KvsData.Value value)
            throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%s", TABLE_NAME, KEY_NAME, keystr);
            GetResult get = null;
            try (var rs = tx.executeQuery(st).await()) {
                get = convert(rs);
            }
            tx.commit().await();
            assertEquals(1, get.size());
            checkRecord(key, value, get.asRecord());
        }
    }

    private static void checkSQLnotFound(SqlClient sql, String keystr) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%s", TABLE_NAME, KEY_NAME, keystr);
            GetResult get = null;
            try (var rs = tx.executeQuery(st).await()) {
                get = convert(rs);
            }
            tx.commit().await();
            assertEquals(0, get.size());
        }
    }

    private static void checkSQLisNull(SqlClient sql, String keystr) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%s", TABLE_NAME, KEY_NAME, keystr);
            int n = 0;
            try (var rs = tx.executeQuery(st).await()) {
                while (rs.nextRow()) {
                    while (rs.nextColumn()) {
                        if (n != 1) {
                            assertEquals(false, rs.isNull());
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

    private static void checkKVSrecord(KvsClient kvs, KvsData.Value key, KvsData.Value value) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, TABLE_NAME, buffer).await();
            kvs.commit(tx).await();
            assertEquals(1, get.size());
            checkRecord(key, value, get.asRecord());
        }
    }

    private static void checkKVSnotFound(KvsClient kvs, KvsData.Value key) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, TABLE_NAME, buffer).await();
            kvs.commit(tx).await();
            assertEquals(0, get.size());
        }
    }

    private static void checkKVSisNull(KvsClient kvs, KvsData.Value key) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, TABLE_NAME, buffer).await();
            kvs.commit(tx).await();
            assertEquals(1, get.size());
            var record = get.asRecord();
            assertEquals(key, record.getEntity().getValues(0));
            assertEquals(true, record.isNull(VALUE_NAME));
        }
    }

    private static void checkRecord(SqlClient sql, KvsClient kvs, SqlValue key, SqlValue value) throws Exception {
        checkSQLrecord(sql, key.value, key.str, value.value);
        checkKVSrecord(kvs, key.value, value.value);
    }

    private static void checkNotFound(SqlClient sql, KvsClient kvs, SqlValue key) throws Exception {
        checkSQLnotFound(sql, key.str);
        checkKVSnotFound(kvs, key.value);
    }

    private static void checkIsNull(SqlClient sql, KvsClient kvs, SqlValue key) throws Exception {
        checkSQLisNull(sql, key.str);
        checkKVSisNull(kvs, key.value);
    }

    private static void checkNonNull(SqlValue key1, SqlValue value1, SqlValue key2, SqlValue value2) throws Exception {
        try (var session = getNewSession(); var sql = SqlClient.attach(session); var kvs = KvsClient.attach(session)) {
            // SQL INSERT
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("INSERT INTO %s (%s, %s) VALUES(%s, %s)", TABLE_NAME, KEY_NAME, VALUE_NAME,
                        key1.str, value1.str);
                System.err.println(st);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value1);
            // KVS PUT (OVERWRITE)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key1.value);
                buffer.add(VALUE_NAME, value2.value);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value2);

            // KVS PUT (INSERT)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2.value);
                buffer.add(VALUE_NAME, value1.value);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value2); // not changed
            checkRecord(sql, kvs, key2, value1); // newly inserted

            // SQL UPDATE
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("UPDATE %s SET %s=%s WHERE %s=%s", TABLE_NAME, VALUE_NAME, value2.str, KEY_NAME,
                        key2.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value2); // not changed
            checkRecord(sql, kvs, key2, value2); // value updated

            // SQL REMOVE
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("DELETE FROM %s WHERE %s=%s", TABLE_NAME, KEY_NAME, key1.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkNotFound(sql, kvs, key1); // deleted
            checkRecord(sql, kvs, key2, value2); // not changed

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

    private static void checkWithNull(SqlValue key1, SqlValue value1, SqlValue key2, SqlValue value2) throws Exception {
        try (var session = getNewSession(); var sql = SqlClient.attach(session); var kvs = KvsClient.attach(session)) {
            // SQL INSERT (value is null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("INSERT INTO %s (%s) VALUES(%s)", TABLE_NAME, KEY_NAME, key1.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkIsNull(sql, kvs, key1);

            // KVS INSERT (value is null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2.value);
                buffer.addNull(VALUE_NAME);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkIsNull(sql, kvs, key1);
            checkIsNull(sql, kvs, key2);

            // SQL UPDATE (make value non-null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("UPDATE %s SET %s=%s WHERE %s=%s", TABLE_NAME, VALUE_NAME, value1.str, KEY_NAME,
                        key1.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkRecord(sql, kvs, key1, value1); // null -> value1
            checkIsNull(sql, kvs, key2);

            // KVS UPDATE (make value non-null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2.value);
                buffer.add(VALUE_NAME, value2.value);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkRecord(sql, kvs, key1, value1);
            checkRecord(sql, kvs, key2, value2); // null -> value2

            // SQL UPDATE (make value null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("UPDATE %s SET %s=NULL WHERE %s=%s", TABLE_NAME, VALUE_NAME, KEY_NAME, key1.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkIsNull(sql, kvs, key1); // value1 -> null
            checkRecord(sql, kvs, key2, value2);

            // KVS UPDATE (make value null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2.value);
                buffer.addNull(VALUE_NAME);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            checkIsNull(sql, kvs, key1);
            checkIsNull(sql, kvs, key2); // value2 -> null

            // SQL DELETE (value is null)
            try (var tx = sql.createTransaction().await()) {
                var st = String.format("DELETE FROM %s WHERE %s=%s", TABLE_NAME, KEY_NAME, key1.str);
                tx.executeStatement(st).await();
                tx.commit().await();
            }
            checkNotFound(sql, kvs, key1); // deleted
            checkIsNull(sql, kvs, key2);

            // KVS REMOVE (value is null)
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, key2.value);
                var remove = kvs.remove(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, remove.size());
            }
            checkNotFound(sql, kvs, key1);
            checkNotFound(sql, kvs, key2); // deleted
        }
    }

    private static void check(String typeName, KvsData.Value key1, KvsData.Value value1, KvsData.Value key2,
            KvsData.Value value2) throws Exception {
        final SqlValue k1 = new SqlValue(typeName, key1);
        final SqlValue v1 = new SqlValue(typeName, value1);
        final SqlValue k2 = new SqlValue(typeName, key2);
        final SqlValue v2 = new SqlValue(typeName, value2);
        checkNonNull(k1, v1, k2, v2);
        checkWithNull(k1, v1, k2, v2);
    }

    private void checkDataType(String typeName, KvsData.Value key1, KvsData.Value value1, KvsData.Value key2,
            KvsData.Value value2) throws Exception {
        // see jogasaki/docs/value_limit.md
        createTable(TABLE_NAME, schema(typeName));
        check(typeName, key1, value1, key2, value2);
    }

    @Test
    public void intTest() throws Exception {
        checkDataType("int", Values.of(1), Values.of(100), Values.of(2), Values.of(200));
    }

    @Test
    public void longTest() throws Exception {
        checkDataType("bigint", Values.of(1L), Values.of(100L), Values.of(2L), Values.of(200L));
    }

    @Test
    public void floatTest() throws Exception {
        checkDataType("float", Values.of(1.23f), Values.of(123.45f), Values.of(2.34f), Values.of(234.56f));
    }

    @Test
    public void doubleTest() throws Exception {
        checkDataType("double", Values.of(1.23), Values.of(123.45), Values.of(2.34), Values.of(234.56));
    }

    @Test
    public void stringTest() throws Exception {
        checkDataType("string", Values.of("aaa"), Values.of("hello"), Values.of("bbb"), Values.of("today"));
    }

    @Test
    public void decimalTest() throws Exception {
        checkDataType("decimal(5,2)", Values.of(BigDecimal.valueOf(1L)), Values.of(BigDecimal.valueOf(100L)),
                Values.of(BigDecimal.valueOf(2L)), Values.of(BigDecimal.valueOf(200L)));
    }

    @Test
    public void decimal2Test() throws Exception {
        checkDataType("decimal(4,2)", Values.of(new BigDecimal("12.34")), Values.of(new BigDecimal("23.45")),
                Values.of(new BigDecimal("34.56")), Values.of(new BigDecimal("45.67")));
    }

    // NOTE jogasaki not supported yet (use placeholder/parameter)
    // see insert_temporal_types test at
    // jogasaki/test/jogasaki/api/host_variables_test.cpp etc
    public void dateTest() throws Exception {
        checkDataType("date", Values.of(LocalDate.of(2023, 1, 1)), Values.of(LocalDate.of(2023, 2, 2)),
                Values.of(LocalDate.of(2023, 3, 3)), Values.of(LocalDate.of(2023, 4, 4)));
    }

    // see dateTest
    public void timeTest() throws Exception {
        checkDataType("time", Values.of(LocalTime.of(1, 1, 1)), Values.of(LocalTime.of(2, 2, 2)),
                Values.of(LocalTime.of(3, 3, 3)), Values.of(LocalTime.of(4, 4, 4)));
    }

    // see dateTest
    public void timePointTest() throws Exception {
        checkDataType("timestamp", Values.of(LocalDateTime.of(2023, 1, 1, 1, 1, 1)),
                Values.of(LocalDateTime.of(2023, 2, 2, 2, 2, 2)), Values.of(LocalDateTime.of(2023, 3, 3, 3, 3, 3)),
                Values.of(LocalDateTime.of(2023, 4, 4, 4, 4, 4)));
    }
}
