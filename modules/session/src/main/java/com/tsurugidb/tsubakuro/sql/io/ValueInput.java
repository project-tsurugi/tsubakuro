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

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;

/**
 * Retrieves SQL values.
 */
public interface ValueInput extends AutoCloseable {

    /**
     * Peeks {@link EntryType entry type} of the next entry.
     * @return the next data type
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     */
    EntryType peekType() throws IOException, InterruptedException;

    /**
     * Discards the next entry.
     * <p>
     * If {@code deep} is set to {@code true}, this will also discards elements of
     * {@link #readRowBegin() rows} or {@link #readArrayBegin() arrays}.
     * Otherwise {@code deep} is {@code false}, this keeps elements in these structures.
     * </p>
     * <p>
     * IF this reached {@link #readEndOfContents() the end of contents}, this will keep its entry
     * and stop the operation.
     * </p>
     * @param deep whether or not this discards elements of rows and arrays
     * @return {@code true} is the next entry was successfully discarded, or {@code false} if
     *      this reached to end of contents
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     */
    boolean skip(boolean deep) throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#NULL} entry.
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    void readNull() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#INT} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IOException if the next entry is inconsistent value type
     * @see #peekType()
     */
    long readInt() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#FLOAT4} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    float readFloat4() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#FLOAT8} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    double readFloat8() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#DECIMAL} entry.
     * You can read {@link EntryType#INT} entry using this method.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    BigDecimal readDecimal() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#CHARACTER} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    default String readCharacter() throws IOException, InterruptedException {
        return readCharacter(new StringBuilder()).toString();
    }

    /**
     * Reads the next {@link EntryType#CHARACTER} entry and put the contents to the buffer.
     * @param buffer the target buffer
     * @return the passed buffer
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    StringBuilder readCharacter(@Nonnull StringBuilder buffer) throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#OCTET} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    default byte[] readOctet() throws IOException, InterruptedException {
        return readOctet(new ByteBuilder()).build();
    }

    /**
     * Reads the next {@link EntryType#OCTET} entry and put the contents to the buffer.
     * @param buffer the target buffer
     * @return the passed buffer
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    ByteBuilder readOctet(@Nonnull ByteBuilder buffer) throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#BIT} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    default boolean[] readBit() throws IOException, InterruptedException {
        return readBit(new BitBuilder()).build();
    }

    /**
     * Reads the next {@link EntryType#BIT} entry and put the contents to the buffer.
     * @param buffer the target buffer
     * @return the passed buffer
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    BitBuilder readBit(@Nonnull BitBuilder buffer) throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#DATE} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    LocalDate readDate() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#TIME_OF_DAY} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    LocalTime readTimeOfDay() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#TIME_POINT} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    LocalDateTime readTimePoint() throws IOException, InterruptedException;


        /**
     * Reads the next {@link EntryType#TIME_OF_DAY_WITH_TIME_ZONE} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    OffsetTime readTimeOfDayWithTimeZone() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#TIME_POINT_WITH_TIME_ZONE} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    OffsetDateTime readTimePointWithTimeZone() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#DATETIME_INTERVAL} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    DateTimeInterval readDateTimeInterval() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#ROW} entry.
     * @return the number of row elements
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    int readRowBegin() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#ARRAY} entry.
     * @return the number of array elements
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    int readArrayBegin() throws IOException, InterruptedException;

    /**
     * Reads the next {@link EntryType#BLOB} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     *
     * @since 1.8.0
     */
    default BlobReference readBlob() throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads the next {@link EntryType#CLOB} entry.
     * @return the value
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     *
     * @since 1.8.0
     */
    default ClobReference readClob() throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads the next {@link EntryType#END_OF_CONTENTS} entry.
     * @throws IOException if I/O error was occurred while reading the contents
     * @throws InterruptedException if interrupted while reading the contents
     * @throws IllegalStateException if the next entry is inconsistent value type
     * @see #peekType()
     */
    void readEndOfContents() throws IOException, InterruptedException;

    @Override
    void close() throws IOException, InterruptedException;
}
