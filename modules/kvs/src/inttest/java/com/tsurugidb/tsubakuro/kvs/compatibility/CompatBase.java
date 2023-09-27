package com.tsurugidb.tsubakuro.kvs.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;

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

class CompatBase extends TestBase {

    final String tableName;
    final int decimalScale;

    static final String KEY_NAME = "k1";
    static final String KEY2_NAME = "k2";
    static final String VALUE_NAME = "v1";
    static final String VALUE2_NAME = "v2";

    CompatBase(String tableName, int decimalScale) {
        this.tableName = tableName;
        this.decimalScale = decimalScale;
    }

    CompatBase(String tableName) {
        this(tableName, 2);
    }

    static String schema(String typeName) {
        return String.format("%s %s PRIMARY KEY, %s %s", KEY_NAME, typeName, VALUE_NAME, typeName);
    }

    static String schemaV2(String typeName) {
        return String.format("%s %s PRIMARY KEY, %s %s, %s %s", KEY_NAME, typeName, VALUE_NAME, typeName, VALUE2_NAME,
                typeName);
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

    private BigDecimal toBigDecimal(KvsData.Value v) {
        return toBigDecimal(v, decimalScale);
    }

    private void checkValue(KvsData.Value expected, KvsData.Value value) throws Exception {
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

    private static final int IDX_KEY = 0; // TODO maybe change
    private static final int IDX_VALUE = 1;
    private static final int IDX_VALUE2 = 2;

    private void checkRecord(KvsData.Value key, KvsData.Value value, Record record) throws Exception {
        assertEquals(KEY_NAME, record.getName(IDX_KEY));
        assertEquals(VALUE_NAME, record.getName(IDX_VALUE));
        checkValue(key, record.getEntity().getValues(IDX_KEY));
        checkValue(value, record.getEntity().getValues(IDX_VALUE));
    }

    private void checkRecord(KvsData.Value key, KvsData.Value value, KvsData.Value value2, Record record)
            throws Exception {
        checkRecord(key, value, record);
        assertEquals(VALUE2_NAME, record.getName(IDX_VALUE2));
        checkValue(value2, record.getEntity().getValues(IDX_VALUE2));
    }

    private GetResult sqlSelect(SqlClient sql, String keystr) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%s", tableName, KEY_NAME, keystr);
            GetResult get = null;
            try (var rs = tx.executeQuery(st).await()) {
                get = convert(rs);
            }
            tx.commit().await();
            return get;
        }
    }

    private void checkSQLrecord(SqlClient sql, KvsData.Value key, String keystr, KvsData.Value value) throws Exception {
        var get = sqlSelect(sql, keystr);
        assertEquals(1, get.size());
        checkRecord(key, value, get.asRecord());
    }

    private void checkSQLrecord(SqlClient sql, KvsData.Value key, String keystr, KvsData.Value value,
            KvsData.Value value2) throws Exception {
        var get = sqlSelect(sql, keystr);
        assertEquals(1, get.size());
        checkRecord(key, value, value2, get.asRecord());
    }

    private void checkSQLnotFound(SqlClient sql, String keystr) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%s", tableName, KEY_NAME, keystr);
            GetResult get = null;
            try (var rs = tx.executeQuery(st).await()) {
                get = convert(rs);
            }
            tx.commit().await();
            assertEquals(0, get.size());
        }
    }

    private void checkSQLisNull(SqlClient sql, String keystr) throws Exception {
        try (var tx = sql.createTransaction().await()) {
            var st = String.format("SELECT * FROM %s WHERE %s=%s", tableName, KEY_NAME, keystr);
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

    private GetResult kvsGet(KvsClient kvs, KvsData.Value key) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, tableName, buffer).await();
            kvs.commit(tx).await();
            return get;
        }
    }

    private void checkKVSrecord(KvsClient kvs, KvsData.Value key, KvsData.Value value) throws Exception {
        var get = kvsGet(kvs, key);
        assertEquals(1, get.size());
        checkRecord(key, value, get.asRecord());
    }

    private void checkKVSrecord(KvsClient kvs, KvsData.Value key, KvsData.Value value, KvsData.Value value2)
            throws Exception {
        var get = kvsGet(kvs, key);
        assertEquals(1, get.size());
        checkRecord(key, value, value2, get.asRecord());
    }

    private void checkKVSnotFound(KvsClient kvs, KvsData.Value key) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, tableName, buffer).await();
            kvs.commit(tx).await();
            assertEquals(0, get.size());
        }
    }

    private void checkKVSisNull(KvsClient kvs, KvsData.Value key) throws Exception {
        try (var tx = kvs.beginTransaction().await()) {
            RecordBuffer buffer = new RecordBuffer();
            buffer.add(KEY_NAME, key);
            var get = kvs.get(tx, tableName, buffer).await();
            kvs.commit(tx).await();
            assertEquals(1, get.size());
            var record = get.asRecord();
            assertEquals(key, record.getEntity().getValues(0));
            assertEquals(true, record.isNull(VALUE_NAME));
        }
    }

    void checkRecord(SqlClient sql, KvsClient kvs, SqlValue key, SqlValue value) throws Exception {
        checkSQLrecord(sql, key.value, key.str, value.value);
        checkKVSrecord(kvs, key.value, value.value);
    }

    void checkRecord(SqlClient sql, KvsClient kvs, SqlValue key, SqlValue value, SqlValue value2) throws Exception {
        checkSQLrecord(sql, key.value, key.str, value.value, value2.value);
        checkKVSrecord(kvs, key.value, value.value, value2.value);
    }

    void checkNotFound(SqlClient sql, KvsClient kvs, SqlValue key) throws Exception {
        checkSQLnotFound(sql, key.str);
        checkKVSnotFound(kvs, key.value);
    }

    void checkIsNull(SqlClient sql, KvsClient kvs, SqlValue key) throws Exception {
        checkSQLisNull(sql, key.str);
        checkKVSisNull(kvs, key.value);
    }

}
