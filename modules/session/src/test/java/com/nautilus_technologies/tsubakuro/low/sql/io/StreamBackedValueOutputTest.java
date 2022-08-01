package com.nautilus_technologies.tsubakuro.low.sql.io;

import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_ARRAY;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_BIT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_CHARACTER;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_DATE;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_DATETIME_INTERVAL;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_DECIMAL;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_DECIMAL_COMPACT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_EMBED_ARRAY;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_EMBED_BIT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_EMBED_CHARACTER;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_EMBED_NEGATIVE_INT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_EMBED_OCTET;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_EMBED_POSITIVE_INT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_EMBED_ROW;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_END_OF_CONTENTS;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_FLOAT4;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_FLOAT8;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_INT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_OCTET;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_ROW;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_TIME_OF_DAY;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_TIME_POINT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.HEADER_UNKNOWN;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.MAX_DECIMAL_COMPACT_COEFFICIENT;
import static com.nautilus_technologies.tsubakuro.low.sql.io.Constants.MIN_DECIMAL_COMPACT_COEFFICIENT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class StreamBackedValueOutputTest {

    @Test
    void writeNull() {
        assertArrayEquals(
                bytes(HEADER_UNKNOWN),
                perform(o -> o.writeNull()));
    }

    @Test
    void writeInt_embed_positive() {
        assertArrayEquals(
                bytes(HEADER_EMBED_POSITIVE_INT),
                perform(o -> o.writeInt(0)));

        assertArrayEquals(
                bytes(HEADER_EMBED_POSITIVE_INT + 63),
                perform(o -> o.writeInt(63)));
    }

    @Test
    void writeInt_embed_negative() {
        assertArrayEquals(
                bytes(HEADER_EMBED_NEGATIVE_INT),
                perform(o -> o.writeInt(-16)));

        assertArrayEquals(
                bytes(HEADER_EMBED_NEGATIVE_INT + 15),
                perform(o -> o.writeInt(-1)));
    }

    @Test
    void writeInt_full() {
        assertArrayEquals(
                sequence(HEADER_INT, sint(64)),
                perform(o -> o.writeInt(64)));
        assertArrayEquals(
                sequence(HEADER_INT, sint(-17)),
                perform(o -> o.writeInt(-17)));

        assertArrayEquals(
                sequence(HEADER_INT, sint(1000)),
                perform(o -> o.writeInt(1000)));
        assertArrayEquals(
                sequence(HEADER_INT, sint(-1000)),
                perform(o -> o.writeInt(-1000)));

        assertArrayEquals(
                sequence(HEADER_INT, sint(Long.MAX_VALUE)),
                perform(o -> o.writeInt(Long.MAX_VALUE)));
        assertArrayEquals(
                sequence(HEADER_INT, sint(Long.MIN_VALUE)),
                perform(o -> o.writeInt(Long.MIN_VALUE)));
    }

    @Test
    void writeFloat4() {
        assertArrayEquals(
                sequence(HEADER_FLOAT4, fixed4(Float.floatToIntBits(1.25f))),
                perform(o -> o.writeFloat4(1.25f)));
        assertArrayEquals(
                sequence(HEADER_FLOAT4, fixed4(Float.floatToIntBits((float) Math.PI))),
                perform(o -> o.writeFloat4((float) Math.PI)));
    }

    @Test
    void writeFloat8() {
        assertArrayEquals(
                sequence(HEADER_FLOAT8, fixed8(Double.doubleToLongBits(1.25d))),
                perform(o -> o.writeFloat8(1.25d)));
        assertArrayEquals(
                sequence(HEADER_FLOAT8, fixed8(Double.doubleToLongBits(Math.E))),
                perform(o -> o.writeFloat8(Math.E)));
    }

    @Test
    void writeDecimal_int() {
        assertArrayEquals(
                perform(o -> o.writeInt(0)),
                perform(o -> o.writeDecimal(BigDecimal.valueOf(0))));

        assertArrayEquals(
                perform(o -> o.writeInt(Long.MAX_VALUE)),
                perform(o -> o.writeDecimal(BigDecimal.valueOf(Long.MAX_VALUE))));

        assertArrayEquals(
                perform(o -> o.writeInt(Long.MIN_VALUE)),
                perform(o -> o.writeDecimal(BigDecimal.valueOf(Long.MIN_VALUE))));
    }

    @Test
    void writeDecimal_compact() {
        assertArrayEquals(
                sequence(HEADER_DECIMAL_COMPACT, sint(-4), sint(-31415L)),
                perform(o -> o.writeDecimal(new BigDecimal("-3.1415"))));
        assertArrayEquals(
                sequence(HEADER_DECIMAL_COMPACT, sint(+10), sint(Long.MIN_VALUE)),
                perform(o -> o.writeDecimal(BigDecimal.valueOf(Long.MIN_VALUE, -10))));
        assertArrayEquals(
                sequence(HEADER_DECIMAL_COMPACT, sint(-10), sint(Long.MAX_VALUE)),
                perform(o -> o.writeDecimal(BigDecimal.valueOf(Long.MAX_VALUE, 10))));
    }

    @Test
    void writeDecimal_full() {
        assertArrayEquals(
                sequence(HEADER_DECIMAL, sint(1), uint(9), bytes(0, 0x80, 0, 0, 0, 0, 0, 0, 0)),
                perform(o -> o.writeDecimal(new BigDecimal(MAX_DECIMAL_COMPACT_COEFFICIENT.add(BigInteger.ONE), -1))));
        assertArrayEquals(
                sequence(HEADER_DECIMAL, sint(1), uint(9), bytes(0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)),
                perform(o -> o.writeDecimal(new BigDecimal(MIN_DECIMAL_COMPACT_COEFFICIENT.subtract(BigInteger.ONE), -1))));
    }

    @Test
    void writeCharacter_embed() {
        assertArrayEquals(
                sequence(HEADER_EMBED_CHARACTER + 1 - 1, nCharacter(1).getBytes(StandardCharsets.UTF_8)),
                perform(o -> o.writeCharacter(nCharacter(1))));
        assertArrayEquals(
                sequence(HEADER_EMBED_CHARACTER + 64 - 1, nCharacter(64).getBytes(StandardCharsets.UTF_8)),
                perform(o -> o.writeCharacter(nCharacter(64))));
    }

    @Test
    void writeCharacter_full() {
        assertArrayEquals(
                sequence(HEADER_CHARACTER, uint(0)),
                perform(o -> o.writeCharacter("")));
        assertArrayEquals(
                sequence(HEADER_CHARACTER, uint(65), nCharacter(65).getBytes(StandardCharsets.UTF_8)),
                perform(o -> o.writeCharacter(nCharacter(65))));
        assertArrayEquals(
                sequence(HEADER_CHARACTER, uint(4096), nCharacter(4096).getBytes(StandardCharsets.UTF_8)),
                perform(o -> o.writeCharacter(nCharacter(4096))));
    }

    @Test
    void writeCharacter_utf8() {
        String s = "\u3042\u3044\u3046\u3048\u304a"; //$NON-NLS-1$
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(
                sequence(HEADER_EMBED_CHARACTER + b.length - 1, b),
                perform(o -> o.writeCharacter(s)));
    }

    @Test
    void writeOctet_embed() {
        assertArrayEquals(
                sequence(HEADER_EMBED_OCTET + 1 - 1, nOctet(1)),
                perform(o -> o.writeOctet(nOctet(1))));
        assertArrayEquals(
                sequence(HEADER_EMBED_OCTET + 16 - 1, nOctet(16)),
                perform(o -> o.writeOctet(nOctet(16))));
    }

    @Test
    void writeOctet_full() {
        assertArrayEquals(
                sequence(HEADER_OCTET, uint(0)),
                perform(o -> o.writeOctet(nOctet(0))));
        assertArrayEquals(
                sequence(HEADER_OCTET, uint(17), nOctet(17)),
                perform(o -> o.writeOctet(nOctet(17))));
        assertArrayEquals(
                sequence(HEADER_OCTET, uint(4096), nOctet(4096)),
                perform(o -> o.writeOctet(nOctet(4096))));
    }

    @Test
    void writeOctet_builder() {
        assertArrayEquals(
                perform(o -> o.writeOctet(nOctet(16))),
                perform(o -> o.writeOctet(new ByteBuilder(nOctet(16)))));
        assertArrayEquals(
                perform(o -> o.writeOctet(nOctet(4096))),
                perform(o -> o.writeOctet(new ByteBuilder(nOctet(4096)))));
    }

    @Test
    void writeBit_embed() {
        assertArrayEquals(
                sequence(HEADER_EMBED_BIT + 1 - 1, bytes(nBit(1))),
                perform(o -> o.writeBit(nBit(1))));
        assertArrayEquals(
                sequence(HEADER_EMBED_BIT + 8 - 1, bytes(nBit(8))),
                perform(o -> o.writeBit(nBit(8))));
    }

    @Test
    void writeBit_full() {
        assertArrayEquals(
                sequence(HEADER_BIT, uint(0)),
                perform(o -> o.writeBit(nBit(0))));
        assertArrayEquals(
                sequence(HEADER_BIT, uint(9), bytes(nBit(9))),
                perform(o -> o.writeBit(nBit(9))));
        assertArrayEquals(
                sequence(HEADER_BIT, uint(13), bytes(nBit(13))),
                perform(o -> o.writeBit(nBit(13))));
        assertArrayEquals(
                sequence(HEADER_BIT, uint(4096), bytes(nBit(4096))),
                perform(o -> o.writeBit(nBit(4096))));
    }

    @Test
    void writeBit_array_embed() {
        assertArrayEquals(
                sequence(HEADER_EMBED_BIT + 1 - 1, bytes(nBit(1))),
                perform(o -> o.writeBit(nBit(1).build())));
        assertArrayEquals(
                sequence(HEADER_EMBED_BIT + 8 - 1, bytes(nBit(8))),
                perform(o -> o.writeBit(nBit(8).build())));
    }

    @Test
    void writeBit_array_full() {
        assertArrayEquals(
                sequence(HEADER_BIT, uint(0)),
                perform(o -> o.writeBit(nBit(0).build())));
        assertArrayEquals(
                sequence(HEADER_BIT, uint(9), bytes(nBit(9))),
                perform(o -> o.writeBit(nBit(9).build())));
        assertArrayEquals(
                sequence(HEADER_BIT, uint(13), bytes(nBit(13))),
                perform(o -> o.writeBit(nBit(13).build())));
        assertArrayEquals(
                sequence(HEADER_BIT, uint(4096), bytes(nBit(4096))),
                perform(o -> o.writeBit(nBit(4096).build())));
    }

    @Test
    void writeDate() {
        assertArrayEquals(
                sequence(HEADER_DATE, sint(0)),
                perform(o -> o.writeDate(LocalDate.ofEpochDay(0))));
        assertArrayEquals(
                sequence(HEADER_DATE, sint(+4096)),
                perform(o -> o.writeDate(LocalDate.ofEpochDay(+4096))));
        assertArrayEquals(
                sequence(HEADER_DATE, sint(-4096)),
                perform(o -> o.writeDate(LocalDate.ofEpochDay(-4096))));
        assertArrayEquals(
                sequence(HEADER_DATE, sint(LocalDate.of(2022, Month.MAY, 4).toEpochDay())),
                perform(o -> o.writeDate(LocalDate.of(2022, Month.MAY, 4))));
    }

    @Test
    void writeTimeOfDay() {
        assertArrayEquals(
                sequence(HEADER_TIME_OF_DAY, uint(0)),
                perform(o -> o.writeTimeOfDay(LocalTime.ofSecondOfDay(0))));
        assertArrayEquals(
                sequence(HEADER_TIME_OF_DAY, uint(TimeUnit.SECONDS.toNanos(4096))),
                perform(o -> o.writeTimeOfDay(LocalTime.ofSecondOfDay(4096))));
        assertArrayEquals(
                sequence(HEADER_TIME_OF_DAY, uint(TimeUnit.DAYS.toNanos(1) - 1)),
                perform(o -> o.writeTimeOfDay(LocalTime.of(23, 59, 59, 999_999_999))));
    }

    @Test
    void writeTimePoint() {
        assertArrayEquals(
                sequence(HEADER_TIME_POINT, sint(0), uint(0)),
                perform(o -> o.writeTimePoint(Instant.ofEpochSecond(0, 0))));
        assertArrayEquals(
                sequence(HEADER_TIME_POINT, sint(+4096), uint(0)),
                perform(o -> o.writeTimePoint(Instant.ofEpochSecond(+4096, 0))));
        assertArrayEquals(
                sequence(HEADER_TIME_POINT, sint(-4096), uint(0)),
                perform(o -> o.writeTimePoint(Instant.ofEpochSecond(-4096, 0))));
        assertArrayEquals(
                sequence(HEADER_TIME_POINT, sint(0), uint(123_456_789)),
                perform(o -> o.writeTimePoint(Instant.ofEpochSecond(0, 123_456_789))));
    }

    @Test
    void writeDateTimeInterval() {
        assertArrayEquals(
                sequence(HEADER_DATETIME_INTERVAL, sint(0), sint(0), sint(0), sint(0)),
                perform(o -> o.writeDateTimeInterval(new DateTimeInterval(0, 0, 0, 0))));
        assertArrayEquals(
                sequence(HEADER_DATETIME_INTERVAL, sint(1), sint(2), sint(3), sint(4)),
                perform(o -> o.writeDateTimeInterval(new DateTimeInterval(1, 2, 3, 4))));
        assertArrayEquals(
                sequence(HEADER_DATETIME_INTERVAL, sint(-1), sint(-2), sint(-3), sint(-4)),
                perform(o -> o.writeDateTimeInterval(new DateTimeInterval(-1, -2, -3, -4))));
    }

    @Test
    void writeRowBegin_embed() {
        assertArrayEquals(
                sequence(HEADER_EMBED_ROW + 1 - 1),
                perform(o -> o.writeRowBegin(1)));
        assertArrayEquals(
                sequence(HEADER_EMBED_ROW + 32 - 1),
                perform(o -> o.writeRowBegin(32)));
    }

    @Test
    void writeRowBegin_full() {
        assertArrayEquals(
                sequence(HEADER_ROW, uint(0)),
                perform(o -> o.writeRowBegin(0)));
        assertArrayEquals(
                sequence(HEADER_ROW, uint(33)),
                perform(o -> o.writeRowBegin(33)));
        assertArrayEquals(
                sequence(HEADER_ROW, uint(4096)),
                perform(o -> o.writeRowBegin(4096)));
    }

    @Test
    void writeArrayBegin_embed() {
        assertArrayEquals(
                sequence(HEADER_EMBED_ARRAY + 1 - 1),
                perform(o -> o.writeArrayBegin(1)));
        assertArrayEquals(
                sequence(HEADER_EMBED_ARRAY + 32 - 1),
                perform(o -> o.writeArrayBegin(32)));
    }

    @Test
    void writeArrayBegin_full() {
        assertArrayEquals(
                sequence(HEADER_ARRAY, uint(0)),
                perform(o -> o.writeArrayBegin(0)));
        assertArrayEquals(
                sequence(HEADER_ARRAY, uint(33)),
                perform(o -> o.writeArrayBegin(33)));
        assertArrayEquals(
                sequence(HEADER_ARRAY, uint(4096)),
                perform(o -> o.writeArrayBegin(4096)));
    }

    @Test
    void writeEndOfContents() {
        assertArrayEquals(
                bytes(HEADER_END_OF_CONTENTS),
                perform(o -> o.writeEndOfContents()));
    }

    private static byte[] bytes(int... values) {
        var results = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            results[i] = (byte) values[i];
        }
        return results;
    }

    private static byte[] sequence(int header, byte[]... arrays) {
        int size = 1 + Arrays.stream(arrays)
                .mapToInt(it -> it.length)
                .sum();
        var results = new byte[size];
        int start = 1;
        results[0] = (byte) header;
        for (var array : arrays) {
            System.arraycopy(array, 0, results, start, array.length);
            start += array.length;
        }
        return results;
    }

    private static byte[] bytes(BitBuilder bits) {
        return Arrays.copyOfRange(bits.getData(), 0, bits.getByteSize());
    }

    private static byte[] uint(long value) {
        return dump(o -> Base128Variant.writeUnsigned(value, o));
    }

    private static byte[] sint(long value) {
        return dump(o -> Base128Variant.writeSigned(value, o));
    }

    private static byte[] fixed4(int value) {
        return dump(o -> {
            try (var out = new DataOutputStream(o)) {
                out.writeInt(value);
            }
        });
    }

    private static byte[] fixed8(long value) {
        return dump(o -> {
            try (var out = new DataOutputStream(o)) {
                out.writeLong(value);
            }
        });
    }

    private static String nCharacter(int count) {
        StringBuilder buf = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            buf.append((char) ('A' + i % 26));
        }
        return buf.toString();
    }

    private static byte[] nOctet(int count) {
        var results = new byte[count];
        for (int i = 0; i < count; i++) {
            results[i] = (byte) i;
        }
        return results;
    }

    private static BitBuilder nBit(int count) {
        var bits = new BitBuilder()
                .setSize(count);
        for (int i = 0; i < count; i++) {
            bits.set(i, i % 2 == 1);
        }
        return bits;
    }

    private static byte[] dump(Action<? super ByteArrayOutputStream> action) {
        try (var buf = new ByteArrayOutputStream()) {
            action.perform(buf);
            return buf.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static byte[] perform(Action<StreamBackedValueOutput> action) {
        return dump(buf -> {
            try (var out = new StreamBackedValueOutput(buf)) {
                action.perform(out);
            }
        });
    }

    @FunctionalInterface
    interface Action<T> {
        void perform(T output) throws IOException;
    }
}
