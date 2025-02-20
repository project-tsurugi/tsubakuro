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

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;

/**
 * Writes SQL values.
 */
public interface ValueOutput extends AutoCloseable {

    /**
     * Writes a {@link EntryType#NULL} entry.
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     */
    void writeNull() throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#INT} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     */
    void writeInt(long value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#FLOAT4} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeFloat4(float value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#FLOAT8} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeFloat8(double value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#DECIMAL} entry.
     * You can write a decimal number also using {@link #writeInt(long)}.
     * @param value the value
     * @throws ArithmeticException the value is out of range
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeDecimal(@Nonnull BigDecimal value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#CHARACTER} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeCharacter(@Nonnull String value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#CHARACTER} entry and put the contents to the buffer.
     * @param buffer the target buffer
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    default void writeCharacter(@Nonnull StringBuilder buffer) throws IOException, InterruptedException {
        Objects.requireNonNull(buffer);
        writeCharacter(buffer.toString());
    }

    /**
     * Writes a {@link EntryType#OCTET} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    default void writeOctet(@Nonnull byte[] value) throws IOException, InterruptedException {
        Objects.requireNonNull(value);
        writeOctet(value, 0, value.length);
    }

    /**
     * Writes a {@link EntryType#OCTET} entry.
     * @param value the value
     * @param offset the source offset position on array
     * @param length the number of elements to write
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeOctet(@Nonnull byte[] value, int offset, int length) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#OCTET} entry and put the contents to the buffer.
     * @param buffer the target buffer
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    default void writeOctet(@Nonnull ByteBuilder buffer) throws IOException, InterruptedException {
        Objects.requireNonNull(buffer);
        writeOctet(buffer.getData(), 0, buffer.getSize());
    }

    /**
     * Writes a {@link EntryType#BIT} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    default void writeBit(@Nonnull boolean[] value) throws IOException, InterruptedException {
        Objects.requireNonNull(value);
        writeBit(value, 0, value.length);
    }

    /**
     * Writes a {@link EntryType#BIT} entry.
     * @param value the value
     * @param offset the source offset position on array
     * @param length the number of elements to write
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeBit(@Nonnull boolean[] value, int offset, int length) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#BIT} entry and put the contents to the buffer.
     * @param buffer the target buffer
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeBit(@Nonnull BitBuilder buffer) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#DATE} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeDate(LocalDate value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#TIME_OF_DAY} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeTimeOfDay(LocalTime value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#TIME_POINT} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeTimePoint(LocalDateTime value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#TIME_OF_DAY_WITH_TIME_ZONE} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeTimeOfDayWithTimeZone(OffsetTime value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#TIME_POINT_WITH_TIME_ZONE} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeTimePointWithTimeZone(OffsetDateTime value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#DATETIME_INTERVAL} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeDateTimeInterval(DateTimeInterval value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#ROW} entry.
     * This does not write contents of the row, so that you should write individual elements using {@code writeXXX()}.
     * @param numberOfElements the number of row elements
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeRowBegin(int numberOfElements) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#ARRAY} entry.
     * This does not write contents of the array, so that you should write individual elements using {@code writeXXX()}.
     * @param numberOfElements the number of array elements
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeArrayBegin(int numberOfElements) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#BLOB} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeBlob(BlobReference value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#CLOB} entry.
     * @param value the value
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeClob(ClobReference value) throws IOException, InterruptedException;

    /**
     * Writes a {@link EntryType#END_OF_CONTENTS} entry.
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void writeEndOfContents() throws IOException, InterruptedException;

    /**
     * Flushes buffered contents to the back-end.
     * @throws IOException if I/O error was occurred while writing the contents
     * @throws InterruptedException if interrupted while writing the contents
     */
    void flush() throws IOException, InterruptedException;

    @Override
    void close() throws IOException, InterruptedException;
}
