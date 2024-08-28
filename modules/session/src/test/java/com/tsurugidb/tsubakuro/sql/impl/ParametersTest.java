/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.sql.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlCommon.Bit;
import com.tsurugidb.sql.proto.SqlCommon.TimePoint;
import com.tsurugidb.sql.proto.SqlCommon.TimeOfDayWithTimeZone;
import com.tsurugidb.sql.proto.SqlCommon.TimePointWithTimeZone;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;

class ParametersTest {
    
    private static final String TN = "TESTING";
    
    private static Parameter.Builder param() {
        return Parameter.newBuilder()
            .setName(TN);
    }
    
    @Test
    void ofNull() {
        assertEquals(param().build(), Parameters.ofNull(TN));
    }
    
    @Test
    void ofBoolean() {
        assertEquals(param().setBooleanValue(true).build(), Parameters.of(TN, true));
    }
    
    @Test
    void ofInt() {
        assertEquals(param().setInt4Value(100).build(), Parameters.of(TN, 100));
    }
    
    @Test
    void ofLong() {
        assertEquals(param().setInt8Value(200L).build(), Parameters.of(TN, 200L));
    }
    
    @Test
    void ofFloat() {
        assertEquals(param().setFloat4Value(0.75f).build(), Parameters.of(TN, 0.75f));
    }
    
    @Test
    void ofDouble() {
        assertEquals(param().setFloat8Value(1.25d).build(), Parameters.of(TN, 1.25d));
    }
    
    @Test
    void ofBigdecimal() {
        BigDecimal value = new BigDecimal("3.14");
        assertEquals(
            param().setDecimalValue(
                    SqlCommon.Decimal.newBuilder()
                            .setUnscaledValue(ByteString.copyFrom(BigInteger.valueOf(314).toByteArray()))
                            .setExponent(-2)).build(),
                    Parameters.of(TN, value));
    }
    
    @Test
    void ofString() {
        assertEquals(param().setCharacterValue("Hello").build(), Parameters.of(TN, "Hello"));
    }
    
    @Test
    void ofBytearray() {
        assertEquals(
                     param().setOctetValue(ByteString.copyFrom(new byte[] { 0, 1, 2 })).build(),
                     Parameters.of(TN, new byte[] { 0, 1, 2 }));
    }
    
    @Test
    void ofBooleanArray() {
        assertEquals(
                     param().setBitValue(Bit.newBuilder()
                                         .setPacked(ByteString.copyFrom(new byte[] { 0x2c }))
                                         .setSize(7))
                     .build(),
                     Parameters.of(TN, new boolean[] { false, false, true, true, false, true, false }));
    }
    
    @Test
    void ofBit() {
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
    void ofLocalDate() {
        assertEquals(param().setDateValue(123456).build(), Parameters.of(TN, LocalDate.ofEpochDay(123456)));
    }
    
    @Test
    void ofLocalTime() {
        assertEquals(param().setTimeOfDayValue(123456).build(), Parameters.of(TN, LocalTime.ofNanoOfDay(123456)));
    }
    
    @Test
    void ofLocalDateTime() {
        assertEquals(
                     param().setTimePointValue(TimePoint.newBuilder()
                                               .setOffsetSeconds(100)
                                               .setNanoAdjustment(200))
                     .build(),
                     Parameters.of(TN, LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.ofNanoOfDay(100L * 1000_000_000 + 200))));
    }
    
    @Test
    void ofOffsetTime() {
        assertEquals(
            param().setTimeOfDayWithTimeZoneValue(TimeOfDayWithTimeZone.newBuilder()
                                      .setOffsetNanoseconds(100L * 1000 * 1000 * 1000 + 200)
                                      .setTimeZoneOffset(9 * 60))
            .build(),
            Parameters.of(TN, OffsetTime.of(LocalTime.ofNanoOfDay(100L * 1000 * 1000 * 1000 + 200), ZoneOffset.ofTotalSeconds(9 * 60 * 60))));
    }

    @Test
    void ofOffsetDateTime() {
        assertEquals(
                    param().setTimePointWithTimeZoneValue(TimePointWithTimeZone.newBuilder()
                                        .setOffsetSeconds(100)
                                        .setNanoAdjustment(200)
                                        .setTimeZoneOffset(9 * 60))
            .build(),
            Parameters.of(TN, OffsetDateTime.of(LocalDate.ofEpochDay(0), LocalTime.ofNanoOfDay(1000_000_000L * 100 + 200), ZoneOffset.ofTotalSeconds(9 * 60 * 60))));
    }

    @Test
    void referenceColumnPosition() {
        assertEquals(param().setReferenceColumnPosition(100).build(), Parameters.referenceColumn(TN, 100));
    }
    
    @Test
    void referenceColumnName() {
        assertEquals(param().setReferenceColumnName("COL").build(), Parameters.referenceColumn(TN, "COL"));
    }
}
