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
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.tsurugidb.kvs.proto.KvsData;

class ValuesTest {

    @Test
    void notSet() throws Exception {
        var builder = KvsData.Value.newBuilder();
        assertEquals(null, Values.toObject(builder.build()));
    }

    @Test
    void booleanValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final boolean value = true;
        assertEquals(value, Values.toObject(builder.setBooleanValue(value).build()));
    }

    @Test
    void intValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final int value = 1234;
        assertEquals(value, Values.toObject(builder.setInt4Value(value).build()));
    }

    @Test
    void longValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final long value = 12345678L;
        assertEquals(value, Values.toObject(builder.setInt8Value(value).build()));
    }

    @Test
    void floatValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final float value = 1234.5f;
        assertEquals(value, Values.toObject(builder.setFloat4Value(value).build()));
    }

    @Test
    void doubleValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final double value = 1234.5678;
        assertEquals(value, Values.toObject(builder.setFloat8Value(value).build()));
    }

    @Test
    void decimalValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var list = List.of(BigDecimal.valueOf(0), BigDecimal.valueOf(12345678), BigDecimal.valueOf(-12345678),
                BigDecimal.valueOf(123456, 78), BigDecimal.valueOf(123456, -78), BigDecimal.valueOf(123.456e+20),
                BigDecimal.valueOf(-123.456));
        for (final var value : list) {
            var decimal = KvsData.Decimal.newBuilder()
                    .setUnscaledValue(ByteString.copyFrom(value.unscaledValue().toByteArray()))
                    .setExponent(-value.scale());
            assertEquals(value, Values.toObject(builder.setDecimalValue(decimal).build()));
        }
    }

    @Test
    void stringValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var list = List.of("", "hello", " \t\n", "\0\1\2\3", "日本語", "ÀÁÂÃÄÅÆ");
        for (final var value : list) {
            assertEquals(value, Values.toObject(builder.setCharacterValue(value).build()));
        }
        assertThrows(NullPointerException.class, () -> {
            final String value = null;
            assertEquals(value, Values.toObject(builder.setCharacterValue(value).build()));
        });
    }

    @Test
    void octetValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var list = List.of("", "hello");
        for (final var s : list) {
            final var value = s.getBytes();
            final var value2 = (byte[]) Values.toObject(builder.setOctetValue(ByteString.copyFrom(value)).build());
            assertEquals(value.length, value2.length);
            for (int i = 0; i < value.length; i++) {
                assertEquals(value[i], value2[i]);
            }
        }
    }

    @Test
    void dateValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var list = List.of(LocalDate.of(2023, 5, 22), LocalDate.of(1998, 4, 15), LocalDate.of(2100, 7, 13));
        for (final var value : list) {
            assertEquals(value, Values.toObject(builder.setDateValue(value.toEpochDay()).build()));
        }
    }

    @Test
    void timeOfDayValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var list = List.of(LocalTime.of(12, 34, 56), LocalTime.of(0, 0, 0), LocalTime.of(23, 59, 59));
        for (final var value : list) {
            assertEquals(value, Values.toObject(builder.setTimeOfDayValue(value.toNanoOfDay()).build()));
        }
    }

    @Test
    void timePointValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var list = List.of(LocalDateTime.of(2023, 5, 22, 12, 34, 56));
        for (final var value : list) {
            var time = KvsData.TimePoint.newBuilder().setOffsetSeconds(value.toEpochSecond(ZoneOffset.UTC))
                    .setNanoAdjustment(value.getNano()).build();
            assertEquals(value, Values.toObject(builder.setTimePointValue(time).build()));
        }
    }

    /*
     * NOTE: The unit of the timezone offset of KvsData.TimeOfDayWithTimeZone and TimePointWithTimeZone
     * is *minute*, not *second* as Java ZoneOffset.
     * The value of seconds in Java ZoneOffset will be 0 through KvsData object.
     * We don't test with ZoneOffset.ofHoursMinutes(1, 23, 56) etc.
     */
    private static final List<ZoneOffset> ZONE_OFFSET_LIST = List.of(ZoneOffset.UTC, ZoneOffset.ofHours(9),
            ZoneOffset.ofHours(-1), ZoneOffset.ofHoursMinutes(1, 23));

    @Test
    void timeOfDayWithTimeZoneValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var time = LocalTime.of(12, 34, 56);
        var list = new LinkedList<OffsetTime>();
        for (var zone : ZONE_OFFSET_LIST) {
            list.add(OffsetTime.of(time, zone));
        }
        for (final var value : list) {
            var timeWithZone = KvsData.TimeOfDayWithTimeZone.newBuilder()
                    .setOffsetNanoseconds(value.toLocalTime().toNanoOfDay())
                    .setTimeZoneOffset(value.getOffset().getTotalSeconds() / 60);
            assertEquals(value, Values.toObject(builder.setTimeOfDayWithTimeZoneValue(timeWithZone).build()));
        }
    }

    @Test
    void timePointWithTimeZoneValue() throws Exception {
        var builder = KvsData.Value.newBuilder();
        final var time = LocalDateTime.of(2023, 5, 22, 12, 34, 56);
        var list = new LinkedList<OffsetDateTime>();
        for (var zone : ZONE_OFFSET_LIST) {
            list.add(OffsetDateTime.of(time, zone));
        }
        for (final var value : list) {
            var timeWithZone = KvsData.TimePointWithTimeZone.newBuilder().setOffsetSeconds(value.toEpochSecond())
                    .setNanoAdjustment(value.getNano()).setTimeZoneOffset(value.getOffset().getTotalSeconds() / 60).build();
            assertEquals(value, Values.toObject(builder.setTimePointWithTimeZoneValue(timeWithZone).build()));
        }
    }

}
