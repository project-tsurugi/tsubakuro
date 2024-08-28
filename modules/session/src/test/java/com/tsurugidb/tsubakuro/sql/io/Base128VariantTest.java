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
package com.tsurugidb.tsubakuro.sql.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class Base128VariantTest {

    @Test
    void writeUnsigned() throws Exception {
        assertArrayEquals(bytes(0x00), uint(0b0000000L));
        assertArrayEquals(bytes(0x7f), uint(0b1111111L));

        assertArrayEquals(bytes(0x80, 0x01), uint(0b0000001_0000000L));
        assertArrayEquals(bytes(0xff, 0x7f), uint(0b1111111_1111111L));

        assertArrayEquals(bytes(0x80, 0x80, 0x01), uint(0b0000001_0000000_0000000L));
        assertArrayEquals(bytes(0xff, 0xff, 0x7f), uint(0b1111111_1111111_1111111L));

        assertArrayEquals(bytes(0x80, 0x80, 0x80, 0x01), uint(0b0000001_0000000_0000000_0000000L));
        assertArrayEquals(bytes(0xff, 0xff, 0xff, 0x7f), uint(0b1111111_1111111_1111111_1111111L));

        assertArrayEquals(
                bytes(0x80, 0x80, 0x80, 0x80, 0x01),
                uint(0b0000001_0000000_0000000_0000000_0000000L));
        assertArrayEquals(
                bytes(0xff, 0xff, 0xff, 0xff, 0x7f),
                uint(0b1111111_1111111_1111111_1111111_1111111L));

        assertArrayEquals(
                bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                uint(0b0000001_0000000_0000000_0000000_0000000_0000000L));
        assertArrayEquals(
                bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                uint(0b1111111_1111111_1111111_1111111_1111111_1111111L));

        assertArrayEquals(
                bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                uint(0b0000001_0000000_0000000_0000000_0000000_0000000_0000000L));
        assertArrayEquals(
                bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                uint(0b1111111_1111111_1111111_1111111_1111111_1111111_1111111L));

        assertArrayEquals(
                bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                uint(0b0000001_0000000_0000000_0000000_0000000_0000000_0000000_0000000L));
        assertArrayEquals(
                bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                uint(0b1111111_1111111_1111111_1111111_1111111_1111111_1111111_1111111L));

        assertArrayEquals(
                bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                uint(0b00000001_0000000_0000000_0000000_0000000_0000000_0000000_0000000_0000000L));
        assertArrayEquals(
                bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
                uint(0b11111111_1111111_1111111_1111111_1111111_1111111_1111111_1111111_1111111L));
    }

    @Test
    void writeSigned() throws IOException {
        assertArrayEquals(bytes(0x00), sint(0b0000000L));

        assertArrayEquals(bytes(0x02), sint(+0b0_000001L));
        assertArrayEquals(bytes(0x01), sint(-0b0_000001L));
        assertArrayEquals(bytes(0x7e), sint(+0b0_111111L));
        assertArrayEquals(bytes(0x7f), sint(-0b1_000000L));

        assertArrayEquals(bytes(0x80, 0x01), sint(+0b0_0000001_000000L));
        assertArrayEquals(bytes(0x81, 0x01), sint(-0b0_0000001_000001L));
        assertArrayEquals(bytes(0xfe, 0x7f), sint(+0b0_1111111_111111L));
        assertArrayEquals(bytes(0xff, 0x7f), sint(-0b1_0000000_000000L));

        assertArrayEquals(bytes(0x80, 0x80, 0x01), sint(+0b0_0000001_0000000_000000L));
        assertArrayEquals(bytes(0x81, 0x80, 0x01), sint(-0b0_0000001_0000000_000001L));
        assertArrayEquals(bytes(0xfe, 0xff, 0x7f), sint(+0b0_1111111_1111111_111111L));
        assertArrayEquals(bytes(0xff, 0xff, 0x7f), sint(-0b1_0000000_0000000_000000L));

        assertArrayEquals(bytes(0x80, 0x80, 0x80, 0x01), sint(+0b0_0000001_0000000_0000000_000000L));
        assertArrayEquals(bytes(0x81, 0x80, 0x80, 0x01), sint(-0b0_0000001_0000000_0000000_000001L));
        assertArrayEquals(bytes(0xfe, 0xff, 0xff, 0x7f), sint(+0b0_1111111_1111111_1111111_111111L));
        assertArrayEquals(bytes(0xff, 0xff, 0xff, 0x7f), sint(-0b1_0000000_0000000_0000000_000000L));

        assertArrayEquals(bytes(0x80, 0x80, 0x80, 0x80, 0x01), sint(+0b0_0000001_0000000_0000000_0000000_000000L));
        assertArrayEquals(bytes(0x81, 0x80, 0x80, 0x80, 0x01), sint(-0b0_0000001_0000000_0000000_0000000_000001L));
        assertArrayEquals(bytes(0xfe, 0xff, 0xff, 0xff, 0x7f), sint(+0b0_1111111_1111111_1111111_1111111_111111L));
        assertArrayEquals(bytes(0xff, 0xff, 0xff, 0xff, 0x7f), sint(-0b1_0000000_0000000_0000000_0000000_000000L));

        assertArrayEquals(bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(+0b0_0000001_0000000_0000000_0000000_0000000_000000L));
        assertArrayEquals(bytes(0x81, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(-0b0_0000001_0000000_0000000_0000000_0000000_000001L));
        assertArrayEquals(bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0x7f),
                sint(+0b0_1111111_1111111_1111111_1111111_1111111_111111L));
        assertArrayEquals(bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                sint(-0b1_0000000_0000000_0000000_0000000_0000000_000000L));

        assertArrayEquals(bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(+0b0_0000001_0000000_0000000_0000000_0000000_0000000_000000L));
        assertArrayEquals(bytes(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(-0b0_0000001_0000000_0000000_0000000_0000000_0000000_000001L));
        assertArrayEquals(bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                sint(+0b0_1111111_1111111_1111111_1111111_1111111_1111111_111111L));
        assertArrayEquals(bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                sint(-0b1_0000000_0000000_0000000_0000000_0000000_0000000_000000L));

        assertArrayEquals(bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(+0b0_0000001_0000000_0000000_0000000_0000000_0000000_0000000_000000L));
        assertArrayEquals(bytes(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(-0b0_0000001_0000000_0000000_0000000_0000000_0000000_0000000_000001L));
        assertArrayEquals(bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                sint(+0b0_1111111_1111111_1111111_1111111_1111111_1111111_1111111_111111L));
        assertArrayEquals(bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
                sint(-0b1_0000000_0000000_0000000_0000000_0000000_0000000_0000000_000000L));

        assertArrayEquals(bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(+0b00_0000001_0000000_0000000_0000000_0000000_0000000_0000000_0000000_000000L));
        assertArrayEquals(bytes(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01),
                sint(-0b00_0000001_0000000_0000000_0000000_0000000_0000000_0000000_0000000_000001L));
        assertArrayEquals(bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff), sint(Long.MAX_VALUE));
        assertArrayEquals(bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff), sint(Long.MIN_VALUE));
    }

    private static byte[] uint(long value) throws IOException {
        try (var output = new ByteArrayOutputStream()) {
            int size = Base128Variant.writeUnsigned(value, output);
            byte[] result = output.toByteArray();
            assertEquals(size, result.length);

            try (var input = new ByteArrayInputStream(result)) {
                long restored = Base128Variant.readUnsigned(input);
                assertEquals(value, restored, Arrays.toString(result));
                assertEquals(-1, input.read());
            }

            return result;
        }
    }

    private static byte[] sint(long value) throws IOException {
        try (var output = new ByteArrayOutputStream()) {
            int size = Base128Variant.writeSigned(value, output);
            byte[] result = output.toByteArray();
            assertEquals(size, result.length);

            try (var input = new ByteArrayInputStream(result)) {
                long restored = Base128Variant.readSigned(input);
                assertEquals(value, restored, Arrays.toString(result));
                assertEquals(-1, input.read());
            }

            return result;
        }
    }

    private static byte[] bytes(int... values) {
        var results = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            results[i] = (byte) values[i];
        }
        return results;
    }
}
