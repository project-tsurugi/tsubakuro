package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.Values;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class NotNullDataTypesTest extends TestBase {

    private static final String TABLE_NAME = "table" + NotNullDataTypesTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    private static void checkPutGet(KvsData.Value key1, KvsData.Value value1) throws Exception {
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            // key: null, value: non-null
            try (var tx = kvs.beginTransaction().await()) {
                buffer.addNull(KEY_NAME);
                buffer.add(VALUE_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.put(tx, TABLE_NAME, buffer).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // key: null, value: null
            try (var tx = kvs.beginTransaction().await()) {
                buffer.addNull(KEY_NAME);
                buffer.addNull(VALUE_NAME);
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.put(tx, TABLE_NAME, buffer).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
            // key: non-null, value: null
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                buffer.addNull(VALUE_NAME);
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.put(tx, TABLE_NAME, buffer).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
        }
    }

    private static String schema(String typeName) {
        return String.format("%s %s PRIMARY KEY, %s %s NOT NULL", KEY_NAME, typeName, VALUE_NAME, typeName);
    }

    private void checkDataType(String typeName, KvsData.Value key1, KvsData.Value value1) throws Exception {
        // see jogasaki/docs/value_limit.md
        createTable(TABLE_NAME, schema(typeName));
        checkPutGet(key1, value1);
    }

    @Test
    public void intTest() throws Exception {
        final Integer key1 = 1;
        final Integer value1 = 100;
        checkDataType("int", Values.of(key1), Values.of(value1));
    }

    @Test
    public void longTest() throws Exception {
        final Long key1 = 1L;
        final Long value1 = 100L;
        checkDataType("bigint", Values.of(key1), Values.of(value1));
    }

    @Test
    public void floatTest() throws Exception {
        final Float key1 = 1.0f;
        final Float value1 = 100.0f;
        checkDataType("float", Values.of(key1), Values.of(value1));
    }

    @Test
    public void doubleTest() throws Exception {
        final Double key1 = 1.0;
        final Double value1 = 100.0;
        checkDataType("double", Values.of(key1), Values.of(value1));
    }

    @Test
    public void stringTest() throws Exception {
        final String key1 = "aaa";
        final String value1 = "hello";
        checkDataType("string", Values.of(key1), Values.of(value1));
    }

    @Test
    public void zeroDecimalTest() throws Exception {
        final BigDecimal key1 = new BigDecimal("0");
        final BigDecimal value1 = new BigDecimal("0");
        checkDataType("decimal", Values.of(key1), Values.of(value1));
    }

    @Test
    public void decimalTest() throws Exception {
        final BigDecimal key1 = new BigDecimal("1234");
        final BigDecimal value1 = new BigDecimal("5678");
        checkDataType("decimal", Values.of(key1), Values.of(value1));
    }

    @Test
    public void decimalScaleTest() throws Exception {
        final BigDecimal key1 = new BigDecimal("12.34");
        final BigDecimal value1 = new BigDecimal("56.78");
        createTable(TABLE_NAME, schema("decimal(4,2)"));
        checkPutGet(Values.of(key1), Values.of(value1));
    }

    @Test
    public void dateTest() throws Exception {
        final LocalDate key1 = LocalDate.of(2023, 5, 22);
        final LocalDate value1 = LocalDate.of(2023, 8, 31);
        checkDataType("date", Values.of(key1), Values.of(value1));
    }

    @Test
    public void timeOfDayTest() throws Exception {
        final LocalTime key1 = LocalTime.of(12, 34, 56);
        final LocalTime value1 = LocalTime.of(18, 0, 0, 123456789);
        checkDataType("time", Values.of(key1), Values.of(value1));
    }

    @Test
    public void timePointTest() throws Exception {
        final LocalDateTime key1 = LocalDateTime.of(2023, 5, 22, 12, 34, 56);
        final LocalDateTime value1 = LocalDateTime.of(2023, 8, 31, 15, 24, 11, 123456789);
        checkDataType("timestamp", Values.of(key1), Values.of(value1));
    }
}
