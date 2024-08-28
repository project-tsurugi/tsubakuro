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
package com.tsurugidb.tsubakuro.kvs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

class RecordTest {

    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    @Test
    void empty() throws Exception {
        Record record = new Record();
        assertEquals(0, record.size());
        assertThrows(IndexOutOfBoundsException.class, () -> {
            record.getValue(0);
        });
    }

    @Test
    void nullValueRecord() throws Exception {
        final Object value = null;
        var recBuffer = new RecordBuffer();
        recBuffer.addNull(KEY1);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(value, record.getValue(KEY1));
        assertEquals(true, record.isNull(KEY1));
        assertThrows(IndexOutOfBoundsException.class, () -> {
            record.getValue(1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            record.isNull(KEY2);
        });
        String[] keys = {KEY1, KEY2};
        for (var key : keys) {
            assertThrows(IllegalArgumentException.class, () -> {
                record.getBoolean(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getInt(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getLong(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getFloat(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getDouble(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getCharacter(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getDecimal(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getDate(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getTimeOfDay(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getTimePoint(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getTimeOfDayWithTimeZone(key);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                record.getTimePointWithTimeZone(key);
            });
        }
    }

    @Test
    void booleanRecord() throws Exception {
        final Boolean value = Boolean.TRUE;
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getBoolean(KEY1));
        assertThrows(IndexOutOfBoundsException.class, () -> {
            record.getValue(1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            record.getInt(KEY1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            record.getValue(KEY2);
        });
    }

    @Test
    void intRecord() throws Exception {
        final int value = 1234;
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getInt(KEY1));
        assertThrows(IndexOutOfBoundsException.class, () -> {
            record.getValue(1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            record.getLong(KEY1);
        });
    }

    @Test
    void longRecord() throws Exception {
        final long value = 1234L;
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getLong(KEY1));
        assertThrows(IndexOutOfBoundsException.class, () -> {
            record.getValue(1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            record.getInt(KEY1);
        });
    }

    @Test
    void floatRecord() throws Exception {
        final float value = 1234.56f;
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getFloat(KEY1));
        assertThrows(IndexOutOfBoundsException.class, () -> {
            record.getValue(1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            record.getDouble(KEY1);
        });
    }

    @Test
    void doubleRecord() throws Exception {
        final double value = 1234.5678;
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getDouble(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getFloat(KEY1);
        });
    }

    @Test
    void decimalRecord() throws Exception {
        final BigDecimal value = BigDecimal.valueOf(1234567L);
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getDecimal(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getLong(KEY1);
        });
    }

    @Test
    void stringRecord() throws Exception {
        final String value = "hello";
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getCharacter(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getInt(KEY1);
        });
    }

    private static void checkOctet(byte[] value, Object o) {
        // NOTE: assertEquals(value, value2); will be failed because value != value2
        // even if value.equales(value2).
        byte[] value2 = (byte[]) o;
        assertEquals(value.length, value2.length);
        for (int i = 0; i < value.length; i++) {
            assertEquals(value[i], value2[i]);
        }
    }

    @Test
    void octetRecord() throws Exception {
        final byte[] value = "hello".getBytes();
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        checkOctet(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        checkOctet(value, record.getOctet(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getCharacter(KEY1);
        });
    }

    @Test
    void dateRecord() throws Exception {
        final LocalDate value = LocalDate.of(2023, 5, 22);
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getDate(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getTimeOfDay(KEY1);
        });
    }

    @Test
    void timeOfDayRecord() throws Exception {
        final LocalTime value = LocalTime.of(12, 34, 56, 123456789);
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getTimeOfDay(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getDate(KEY1);
        });
    }

    @Test
    void timePointRecord() throws Exception {
        final LocalDateTime value = LocalDateTime.of(2023, 5, 22, 12, 34, 56, 123456789);
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getTimePoint(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getDate(KEY1);
        });
    }

    @Test
    void timeOfDayWithTimeZoneRecord() throws Exception {
        final var time = LocalTime.of(12, 34, 56);
        final var value = OffsetTime.of(time, ZoneOffset.ofHours(9));
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getTimeOfDayWithTimeZone(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getTimeOfDay(KEY1);
        });
    }

    @Test
    void timePointWithTimeZoneRecord() throws Exception {
        final var date = LocalDateTime.of(2023, 5, 22, 12, 34, 56);
        final var value = OffsetDateTime.of(date, ZoneOffset.ofHours(9));
        var recBuffer = new RecordBuffer();
        recBuffer.add(KEY1, value);
        var record = recBuffer.toRecord();
        assertEquals(1, record.size());
        assertEquals(value, record.getValue(0));
        assertEquals(false, record.isNull(KEY1));
        assertEquals(value, record.getTimePointWithTimeZone(KEY1));
        assertThrows(IllegalArgumentException.class, () -> {
            record.getTimeOfDayWithTimeZone(KEY1);
        });
    }

}
