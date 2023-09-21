package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.kvs.proto.KvsData.Value.ValueCase;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.Values;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class DataTypesTest extends TestBase {

    private static final String TABLE_NAME = "table" + DataTypesTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    private static BigDecimal convert(KvsData.Value v) {
        KvsData.Decimal dec = v.getDecimalValue();
        return new BigDecimal(new BigInteger(dec.getUnscaledValue().toByteArray()), -dec.getExponent());
    }

    private static void checkValue(KvsData.Value expected, KvsData.Value value) throws Exception {
        if (expected.getValueCase() != ValueCase.DECIMAL_VALUE) {
            assertEquals(expected, value);
        } else {
            var expectedDec = convert(expected);
            var valueDec = convert(value);
            assertEquals(expectedDec, valueDec);
            assertEquals(expectedDec.scale(), valueDec.scale());
            assertEquals(expectedDec.toString(), valueDec.toString());
            System.err.println(expectedDec + "\t" + valueDec);
        }
    }

    private static void checkRecord(Record record, KvsData.Value key1, KvsData.Value value1) throws Exception {
        final int idxKey = 0; // TODO maybe change
        final int idxValue = 1;
        assertEquals(KEY_NAME, record.getName(idxKey));
        assertEquals(VALUE_NAME, record.getName(idxValue));
        checkValue(key1, record.getEntity().getValues(idxKey));
        checkValue(value1, record.getEntity().getValues(idxValue));
    }

    private static void checkPutGet(KvsData.Value key1, KvsData.Value value1) throws Exception {
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
                var put = kvs.put(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, put.size());
            }
            try (var tx = kvs.beginTransaction().await()) {
                buffer.clear();
                buffer.add(KEY_NAME, key1);
                var get = kvs.get(tx, TABLE_NAME, buffer).await();
                kvs.commit(tx).await();
                assertEquals(1, get.size());
                checkRecord(get.asRecord(), key1, value1);
            }
        }
    }

    private static void checkPutNG(KvsData.Value key1, KvsData.Value value1, KvsServiceCode code) throws Exception {
        RecordBuffer buffer = new RecordBuffer();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                buffer.add(KEY_NAME, key1);
                buffer.add(VALUE_NAME, value1);
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.put(tx, TABLE_NAME, buffer).await();
                });
                assertEquals(code, ex.getDiagnosticCode());
                kvs.rollback(tx).await();
            }
        }
    }

    private static String schema(String typeName) {
        return String.format("%s %s PRIMARY KEY, %s %s", KEY_NAME, typeName, VALUE_NAME, typeName);
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
        checkPutGet(Values.of(""), Values.of(""));
    }

    @Test
    public void char10Test() throws Exception {
        final String key1 = "1234567890"; // OK
        final String value1 = "abcdefghij"; // OK
        createTable(TABLE_NAME, schema("char(10)"));
        checkPutGet(Values.of(key1), Values.of(value1));
        //
        checkPutGet(Values.of(""), Values.of(""));
        //
        final String key2 = "9876543210"; // OK
        final String value2 = "1234567890A"; // NG: too long
        checkPutNG(Values.of(key2), Values.of(value2), KvsServiceCode.RESOURCE_LIMIT_REACHED);
        checkPutNG(Values.of(value2), Values.of(key2), KvsServiceCode.RESOURCE_LIMIT_REACHED);
        //
        final String key3 = "987654321"; // OK: too short
        checkPutGet(Values.of(key3), Values.of(value1));
        checkPutGet(Values.of(value1), Values.of(key3));
    }

    @Test
    public void varchar10Test() throws Exception {
        final String key1 = "1234567890"; // OK
        final String value1 = "abcdefghij"; // OK
        createTable(TABLE_NAME, schema("varchar(10)"));
        checkPutGet(Values.of(key1), Values.of(value1));
        //
        checkPutGet(Values.of(""), Values.of(""));
        //
        final String key2 = "9876543210"; // OK
        final String value2 = "1234567890A"; // NG: too long
        checkPutNG(Values.of(key2), Values.of(value2), KvsServiceCode.RESOURCE_LIMIT_REACHED);
        checkPutNG(Values.of(value2), Values.of(key2), KvsServiceCode.RESOURCE_LIMIT_REACHED);
        //
        final String key3 = "987654321"; // OK: too short
        checkPutGet(Values.of(key3), Values.of(value1));
        checkPutGet(Values.of(value1), Values.of(key3));
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
        // OK: too short integer part
        checkPutGet(Values.of(new BigDecimal("1.45")), Values.of(new BigDecimal("5.67")));

        // NG: too long fraction part, precision (=4) is OK
        checkPutNG(Values.of(new BigDecimal("1.456")), Values.of(new BigDecimal("5.678")),
                KvsServiceCode.INVALID_ARGUMENT);

        // OK: too short fraction part
        // TODO support short fraction part
        System.err.println("TODO: 'short fraction part' should be acceppted?");
        checkPutNG(Values.of(new BigDecimal("12.3")), Values.of(new BigDecimal("56.7")),
              KvsServiceCode.INVALID_ARGUMENT);
//        checkPutGet(Values.of(new BigDecimal("12.3")), Values.of(new BigDecimal("56.7")));
        checkPutGet(Values.of(new BigDecimal("12.30")), Values.of(new BigDecimal("56.70")));

        // NG: too long integer part
        final BigDecimal key2 = new BigDecimal("123.45");
        final BigDecimal value2 = new BigDecimal("567.89");
        checkPutNG(Values.of(key2), Values.of(value1), KvsServiceCode.INVALID_ARGUMENT);
        checkPutNG(Values.of(key1), Values.of(value2), KvsServiceCode.INVALID_ARGUMENT);
        checkPutNG(Values.of(key2), Values.of(value2), KvsServiceCode.INVALID_ARGUMENT);
        // NG: too long fraction part
        final BigDecimal key3 = new BigDecimal("12.456");
        final BigDecimal value3 = new BigDecimal("56.789");
        checkPutNG(Values.of(key3), Values.of(value1), KvsServiceCode.INVALID_ARGUMENT);
        checkPutNG(Values.of(key1), Values.of(value3), KvsServiceCode.INVALID_ARGUMENT);
        checkPutNG(Values.of(key3), Values.of(value3), KvsServiceCode.INVALID_ARGUMENT);
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
