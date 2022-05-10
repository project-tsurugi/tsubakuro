package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import org.firebirdsql.decimal.Decimal128;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos.Bit;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos.TimePoint;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;

class ParametersTest {

    private static final String TN = "TESTING";

    private static ParameterSet.Parameter.Builder param() {
        return ParameterSet.Parameter.newBuilder()
                .setName(TN);
    }

    @Test
    void ofNull() {
        assertEquals(param().build(), Parameters.ofNull(TN));
    }

    @Test
    void of_boolean() {
        assertEquals(param().setBooleanValue(true).build(), Parameters.of(TN, true));
    }

    @Test
    void of_Int() {
        assertEquals(param().setInt4Value(100).build(), Parameters.of(TN, 100));
    }

    @Test
    void of_Long() {
        assertEquals(param().setInt8Value(200L).build(), Parameters.of(TN, 200L));
    }

    @Test
    void of_Float() {
        assertEquals(param().setFloat4Value(0.75f).build(), Parameters.of(TN, 0.75f));
    }

    @Test
    void of_Double() {
        assertEquals(param().setFloat8Value(1.25d).build(), Parameters.of(TN, 1.25d));
    }

    @Test
    void of_BigDecimal() {
        BigDecimal value = new BigDecimal("3.14");
        assertEquals(
                param().setDecimalValue(ByteString.copyFrom(Decimal128.valueOf(value).toBytes())).build(),
                Parameters.of(TN, value));
    }

    @Test
    void of_String() {
        assertEquals(param().setCharacterValue("Hello").build(), Parameters.of(TN, "Hello"));
    }

    @Test
    void of_ByteArray() {
        assertEquals(
                param().setOctetValue(ByteString.copyFrom(new byte[] { 0, 1, 2 })).build(),
                Parameters.of(TN, new byte[] { 0, 1, 2 }));
    }

    @Test
    void of_BooleanArray() {
        assertEquals(
                param().setBitValue(Bit.newBuilder()
                    .setPacked(ByteString.copyFrom(new byte[] { 0x2c }))
                    .setSize(7))
                    .build(),
                Parameters.of(TN, new boolean[] { false, false, true, true, false, true, false }));
    }

    @Test
    void of_Bit() {
        assertEquals(
                param().setBitValue(Bit.newBuilder()
                    .setPacked(ByteString.copyFrom(new byte[] { 0x2c }))
                    .setSize(7))
                    .build(),
                Parameters.of(TN, Bit.newBuilder()
                        .setPacked(ByteString.copyFrom(new byte[] { 0x2c }))
                        .setSize(7)
                        .build()));
    }

    @Test
    void of_LocalDate() {
        assertEquals(param().setDateValue(123456).build(), Parameters.of(TN, LocalDate.ofEpochDay(123456)));
    }

    @Test
    void of_LocalTime() {
        assertEquals(param().setTimeOfDayValue(123456).build(), Parameters.of(TN, LocalTime.ofNanoOfDay(123456)));
    }

    @Test
    void of_Instant() {
        assertEquals(
                param().setTimePointValue(TimePoint.newBuilder()
                        .setOffsetSeconds(100)
                        .setNanoAdjustment(200))
                        .build(),
                Parameters.of(TN, Instant.ofEpochSecond(100, 200)));
    }

    @Test
    void referenceColumn_position() {
        assertEquals(param().setReferenceColumnPosition(100).build(), Parameters.referenceColumn(TN, 100));
    }

    @Test
    void referenceColumn_name() {
        assertEquals(param().setReferenceColumnName("COL").build(), Parameters.referenceColumn(TN, "COL"));
    }
}
