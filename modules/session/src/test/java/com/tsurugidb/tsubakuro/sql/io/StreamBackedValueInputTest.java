package com.tsurugidb.tsubakuro.sql.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

class StreamBackedValueInputTest {

    @Test
    void readNull() {
        assertSerDe(
                null,
                (output, value) -> output.writeNull(),
                (input) -> {
                    input.readNull();
                    return null;
                });
    }

    @Test
    void readInt_embed_positive() {
        assertSerDe(0L, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
        assertSerDe(63L, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
    }

    @Test
    void readInt_embed_negative() {
        assertSerDe(-1L, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
        assertSerDe(-16L, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
    }

    @Test
    void readInt_full() {
        assertSerDe(64L, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
        assertSerDe(-17L, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
        assertSerDe(Long.MAX_VALUE, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
        assertSerDe(Long.MIN_VALUE, StreamBackedValueOutput::writeInt, StreamBackedValueInput::readInt);
    }

    @Test
    void readFloat4() {
        assertSerDe(0f, StreamBackedValueOutput::writeFloat4, StreamBackedValueInput::readFloat4);
        assertSerDe((float) Math.PI, StreamBackedValueOutput::writeFloat4, StreamBackedValueInput::readFloat4);
        assertSerDe((float) -Math.E, StreamBackedValueOutput::writeFloat4, StreamBackedValueInput::readFloat4);
        assertSerDe(Float.POSITIVE_INFINITY, StreamBackedValueOutput::writeFloat4, StreamBackedValueInput::readFloat4);
        assertSerDe(Float.NEGATIVE_INFINITY, StreamBackedValueOutput::writeFloat4, StreamBackedValueInput::readFloat4);
    }

    @Test
    void readFloat8() {
        assertSerDe(0d, StreamBackedValueOutput::writeFloat8, StreamBackedValueInput::readFloat8);
        assertSerDe(Math.PI, StreamBackedValueOutput::writeFloat8, StreamBackedValueInput::readFloat8);
        assertSerDe(-Math.E, StreamBackedValueOutput::writeFloat8, StreamBackedValueInput::readFloat8);
        assertSerDe(Double.POSITIVE_INFINITY, StreamBackedValueOutput::writeFloat8, StreamBackedValueInput::readFloat8);
        assertSerDe(Double.NEGATIVE_INFINITY, StreamBackedValueOutput::writeFloat8, StreamBackedValueInput::readFloat8);
    }

    @Test
    void readDecimal_int() {
        assertSerDe(BigDecimal.valueOf(0L),
                StreamBackedValueOutput::writeDecimal, StreamBackedValueInput::readDecimal);
        assertSerDe(BigDecimal.valueOf(Long.MAX_VALUE),
                StreamBackedValueOutput::writeDecimal, StreamBackedValueInput::readDecimal);
        assertSerDe(BigDecimal.valueOf(Long.MIN_VALUE),
                StreamBackedValueOutput::writeDecimal, StreamBackedValueInput::readDecimal);
    }

    @Test
    void readDecimal_compact() {
        assertSerDe(new BigDecimal("3.14"),
                StreamBackedValueOutput::writeDecimal, StreamBackedValueInput::readDecimal);
        assertSerDe(new BigDecimal("-1.4142"),
                StreamBackedValueOutput::writeDecimal, StreamBackedValueInput::readDecimal);
    }

    @Test
    void readDecimal_full() {
        assertSerDe(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE),
                StreamBackedValueOutput::writeDecimal, StreamBackedValueInput::readDecimal);
        assertSerDe(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE),
                StreamBackedValueOutput::writeDecimal, StreamBackedValueInput::readDecimal);
    }

    @Test
    void readCharacter_embed() {
        assertSerDe(nCharacter(1), StreamBackedValueOutput::writeCharacter, StreamBackedValueInput::readCharacter);
        assertSerDe(nCharacter(64), StreamBackedValueOutput::writeCharacter, StreamBackedValueInput::readCharacter);
    }

    @Test
    void readCharacter_full() {
        assertSerDe(nCharacter(0), StreamBackedValueOutput::writeCharacter, StreamBackedValueInput::readCharacter);
        assertSerDe(nCharacter(65), StreamBackedValueOutput::writeCharacter, StreamBackedValueInput::readCharacter);
        assertSerDe(nCharacter(4096), StreamBackedValueOutput::writeCharacter, StreamBackedValueInput::readCharacter);
    }

    @Test
    void readCharacter_utf8() {
        assertSerDe("\u3042\u3044\u3046\u3048\u304a",
                StreamBackedValueOutput::writeCharacter, StreamBackedValueInput::readCharacter);
    }

    @Test
    void readCharacter_builder() {
        assertSerDe(nCharacter(4096),
                StreamBackedValueOutput::writeCharacter,
                input -> input.readCharacter(new StringBuilder().append("DUMMY")).toString());
    }

    @Test
    void readOctet_embed() {
        assertSerDe(nOctet(1), StreamBackedValueOutput::writeOctet, StreamBackedValueInput::readOctet);
        assertSerDe(nOctet(16), StreamBackedValueOutput::writeOctet, StreamBackedValueInput::readOctet);
    }

    @Test
    void readOctet_full() {
        assertSerDe(nOctet(0), StreamBackedValueOutput::writeOctet, StreamBackedValueInput::readOctet);
        assertSerDe(nOctet(17), StreamBackedValueOutput::writeOctet, StreamBackedValueInput::readOctet);
        assertSerDe(nOctet(4096), StreamBackedValueOutput::writeOctet, StreamBackedValueInput::readOctet);
    }

    @Test
    void readOctet_builder() {
        assertSerDe(nOctet(4096),
                StreamBackedValueOutput::writeOctet,
                input -> input.readOctet(new ByteBuilder(new byte[] { (byte) 0xca, (byte) 0xfe })).build());
    }

    @Test
    void readBit_embed() {
        assertSerDe(nBit(1), StreamBackedValueOutput::writeBit, StreamBackedValueInput::readBit);
        assertSerDe(nBit(8), StreamBackedValueOutput::writeBit, StreamBackedValueInput::readBit);
    }

    @Test
    void readBit_full() {
        assertSerDe(nBit(0), StreamBackedValueOutput::writeBit, StreamBackedValueInput::readBit);
        assertSerDe(nBit(9), StreamBackedValueOutput::writeBit, StreamBackedValueInput::readBit);
        assertSerDe(nBit(4096), StreamBackedValueOutput::writeBit, StreamBackedValueInput::readBit);
    }

    @Test
    void readBit_builder() {
        assertSerDe(nBit(4096),
                StreamBackedValueOutput::writeBit,
                input -> input.readBit(new BitBuilder(new boolean[] { true, false, false })).build());
    }

    @Test
    void readDate() {
        assertSerDe(LocalDate.of(1970, 1, 1), StreamBackedValueOutput::writeDate, StreamBackedValueInput::readDate);
        assertSerDe(LocalDate.of(2022, 5, 4), StreamBackedValueOutput::writeDate, StreamBackedValueInput::readDate);
    }

    @Test
    void readTimeOfDay() {
        assertSerDe(LocalTime.of(0, 0, 0),
                StreamBackedValueOutput::writeTimeOfDay, StreamBackedValueInput::readTimeOfDay);
        assertSerDe(LocalTime.of(12, 34, 56),
                StreamBackedValueOutput::writeTimeOfDay, StreamBackedValueInput::readTimeOfDay);
        assertSerDe(LocalTime.of(0, 0, 0, 123_456_789),
                StreamBackedValueOutput::writeTimeOfDay, StreamBackedValueInput::readTimeOfDay);
    }

    @Test
    void readTimePoint() {
        assertSerDe(Instant.ofEpochSecond(0, 0),
                StreamBackedValueOutput::writeTimePoint, StreamBackedValueInput::readTimePoint);
        assertSerDe(Instant.parse("2022-05-04T12:34:56.789Z"),
                StreamBackedValueOutput::writeTimePoint, StreamBackedValueInput::readTimePoint);
        assertSerDe(Instant.ofEpochSecond(0, 123_456_789),
                StreamBackedValueOutput::writeTimePoint, StreamBackedValueInput::readTimePoint);
    }

    @Test
    void readDateTimeInterval() {
        assertSerDe(new DateTimeInterval(0, 0, 0, 0),
                StreamBackedValueOutput::writeDateTimeInterval, StreamBackedValueInput::readDateTimeInterval);
        assertSerDe(new DateTimeInterval(1, 2, 3, 4),
                StreamBackedValueOutput::writeDateTimeInterval, StreamBackedValueInput::readDateTimeInterval);
        assertSerDe(new DateTimeInterval(-1, -2, -3, -4),
                StreamBackedValueOutput::writeDateTimeInterval, StreamBackedValueInput::readDateTimeInterval);
    }

    @Test
    void readRowBegin_embed() {
        assertSerDe(1, StreamBackedValueOutput::writeRowBegin, StreamBackedValueInput::readRowBegin);
        assertSerDe(16, StreamBackedValueOutput::writeRowBegin, StreamBackedValueInput::readRowBegin);
    }

    @Test
    void readRowBegin_full() {
        assertSerDe(0, StreamBackedValueOutput::writeRowBegin, StreamBackedValueInput::readRowBegin);
        assertSerDe(17, StreamBackedValueOutput::writeRowBegin, StreamBackedValueInput::readRowBegin);
        assertSerDe(4096, StreamBackedValueOutput::writeRowBegin, StreamBackedValueInput::readRowBegin);
    }

    @Test
    void readArrayBegin_embed() {
        assertSerDe(1, StreamBackedValueOutput::writeArrayBegin, StreamBackedValueInput::readArrayBegin);
        assertSerDe(16, StreamBackedValueOutput::writeArrayBegin, StreamBackedValueInput::readArrayBegin);
    }

    @Test
    void readArrayBegin_full() {
        assertSerDe(0, StreamBackedValueOutput::writeArrayBegin, StreamBackedValueInput::readArrayBegin);
        assertSerDe(17, StreamBackedValueOutput::writeArrayBegin, StreamBackedValueInput::readArrayBegin);
        assertSerDe(4096, StreamBackedValueOutput::writeArrayBegin, StreamBackedValueInput::readArrayBegin);
    }

    @Test
    void read_inconsistent() {
        assertSerDe(
                null,
                (output, value) -> output.writeNull(),
                (input) -> {
                    assertThrows(IllegalStateException.class, () -> input.readInt());
                    input.readNull();
                    return null;
                });
        assertSerDe(
                null,
                (output, value) -> output.writeNull(),
                (input) -> {
                    assertThrows(IllegalStateException.class, () -> input.readDecimal());
                    input.readNull();
                    return null;
                });
    }

    @Test
    void readEndOfContents_empty() {
        deserialize(new byte[0], false, input -> {
            input.readEndOfContents();
            return null;
        });
        deserialize(new byte[0], false, input -> {
            assertFalse(input.skip(false));
            return null;
        });
    }

    @Test
    void readEndOfContents_explicit() {
        byte[] eoc = serialize(null, false, (output, value) -> output.writeEndOfContents());

        deserialize(eoc, false, input -> {
            input.readEndOfContents();
            return null;
        });

        deserialize(eoc, false, input -> {
            assertFalse(input.skip(false));
            return null;
        });
    }

    @Test
    void skip_deep_row() {
        byte[] bytes = serialize(null, true, (output, value) -> {
            output.writeRowBegin(3);
            output.writeInt(1);
            output.writeInt(2);
            output.writeInt(3);
        });

        deserialize(bytes, true, input -> {
            assertTrue(input.skip(true));
            return null;
        });
    }

    @Test
    void skip_deep_row_partial() {
        byte[] bytes = serialize(null, false, (output, value) -> {
            output.writeRowBegin(3);
            output.writeInt(1);
            output.writeInt(2);
        });

        deserialize(bytes, false, input -> {
            assertFalse(input.skip(true));
            return null;
        });
    }

    @Test
    void skip_array_row() {
        byte[] bytes = serialize(null, true, (output, value) -> {
            output.writeArrayBegin(3);
            output.writeInt(1);
            output.writeInt(2);
            output.writeInt(3);
        });

        deserialize(bytes, true, input -> {
            assertTrue(input.skip(true));
            return null;
        });
    }

    @Test
    void skip_deep_array_partial() {
        byte[] bytes = serialize(null, false, (output, value) -> {
            output.writeArrayBegin(3);
            output.writeInt(1);
            output.writeInt(2);
        });

        deserialize(bytes, false, input -> {
            assertFalse(input.skip(true));
            return null;
        });
    }

    @FunctionalInterface
    interface Reader<T> {
        T read(StreamBackedValueInput input) throws IOException;
    }

    @FunctionalInterface
    interface Writer<T> {
        void write(StreamBackedValueOutput output, T value) throws IOException;
    }

    private static <T> byte[] serialize(T value, boolean padding, Writer<T> writer) {
        try (
            var buffer = new ByteArrayOutputStream();
            var output = new StreamBackedValueOutput(buffer);
        ) {
            writer.write(output, value);
            if (padding) {
                output.writeInt(65816);
            }
            output.close();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static <T> T deserialize(byte[] contents, boolean padding, Reader<T> reader) {
        try (var input = new StreamBackedValueInput(new ByteArrayInputStream(contents))) {
            var restored = reader.read(input);
            if (padding) {
                assertEquals(65816, input.readInt());
            }
            input.readEndOfContents();
            return restored;
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void checkSkip(byte[] contents, boolean padding) {
        try (var input = new StreamBackedValueInput(new ByteArrayInputStream(contents))) {
            assertTrue(input.skip(false));
            if (padding) {
                assertEquals(65816, input.readInt());
            }
            input.readEndOfContents();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static <T> void assertSerDe(T value, Writer<T> writer, Reader<T> reader) {
        byte[] bytes = serialize(value, true, writer);
        T restored = deserialize(bytes, true, reader);
        if (restored == null || !restored.getClass().isArray()) {
            assertEquals(value, restored);
        } else if (value instanceof byte[]) {
            assertArrayEquals((byte[]) value, (byte[])restored);
        } else if (value instanceof boolean[]) {
            assertArrayEquals((boolean[]) value, (boolean[])restored);
        } else {
            fail(value.getClass().toString());
        }

        checkSkip(bytes, true);
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

    private static boolean[] nBit(int count) {
        var results = new boolean[count];
        for (int i = 0; i < count; i++) {
            results[i] = (i % 2 == 1) ^ (i % 7 == 1);
        }
        return results;
    }
}
