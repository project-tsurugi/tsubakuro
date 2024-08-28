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
package com.tsurugidb.tsubakuro.sql.impl.testing;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;

// import javax.annotation.Nonnull;
// import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;
import com.tsurugidb.tsubakuro.sql.io.EntryType;
import com.tsurugidb.tsubakuro.sql.io.ValueInput;
import com.tsurugidb.tsubakuro.sql.io.ValueOutput;

/**
 * An entry for {@link ValueInput} and {@link ValueOutput}.
 */
public final class Entry {

    /**
     * An entry of {@link EntryType#NULL}.
     */
    public static final Entry NULL = new Entry(EntryType.NULL, null);

    /**
     * An entry of {@link EntryType#END_OF_CONTENTS}.
     */
    public static final Entry END_OF_CONTENTS = new Entry(EntryType.END_OF_CONTENTS, null);

    private final EntryType type;

    private final Object value;

    private Entry(EntryType type, Object value) {
        assert type != null;
        this.type = type;
        this.value = value;
    }

    /**
     * Returns the entry type.
     * @return the entry type
     */
    public EntryType getType() {
        return type;
    }

    /**
     * Parses an object and returns the corresponding entry.
     *
     * <p>
     * This maps individual values as following:
     * </p>
     *
     * <table>
     *   <thead>
     *     <tr>
     *       <th> pattern </th>
     *       <th> entry type </th>
     *     </tr>
     *   </thead>
     *   <tbody>
     *     <tr>
     *       <td> {@code value == null} </td>
     *       <td> {@link #forNull()} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof Boolean} </td>
     *       <td> {@link #forInt(long)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof Integer} </td>
     *       <td> {@link #forInt(long)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof Long} </td>
     *       <td> {@link #forInt(long)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof Float} </td>
     *       <td> {@link #forFloat4(float)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof Double} </td>
     *       <td> {@link #forFloat8(double)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof BigDecimal} </td>
     *       <td> {@link #forDecimal(BigDecimal)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof String} </td>
     *       <td> {@link #forCharacter(String)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof byte[]} </td>
     *       <td> {@link #forOctet(byte[])} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof boolean[]} </td>
     *       <td> {@link #forBit(boolean[])} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof LocalDate} </td>
     *       <td> {@link #forDate(LocalDate)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof LocalTime} </td>
     *       <td> {@link #forTimeOfDay(LocalTime)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof LocalDateTime} </td>
     *       <td> {@link #forTimePoint(LocalDateTime)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof OffsetTime} </td>
     *       <td> {@link #forTimeOfDayWithTimeZone(OffsetTime)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof OffsetDateTime} </td>
     *       <td> {@link #forTimePointWithTimeZone(OffsetDateTime)} </td>
     *     </tr>
     *     <tr>
     *       <td> {@code value instanceof DateTimeInterval} </td>
     *       <td> {@link #forDateTimeInterval(DateTimeInterval)} </td>
     *     </tr>
     *   </tbody>
     * </table>
     *
     * @param value the input value
     * @return the corresponding entry, or {@code empty} if the input is unrecognized
     */
    public static Optional<Entry> parse(Object value) {
        return Optional.ofNullable(parse0(value));
    }

    private static Entry parse0(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Boolean) {
            return Entry.forInt((Boolean) value ? 1 : 0);
        }
        if (value instanceof Integer) {
            return Entry.forInt((Integer) value);
        }
        if (value instanceof Long) {
            return Entry.forInt((Long) value);
        }
        if (value instanceof Float) {
            return Entry.forFloat4((Float) value);
        }
        if (value instanceof Double) {
            return Entry.forFloat8((Double) value);
        }
        if (value instanceof BigDecimal) {
            return Entry.forDecimal((BigDecimal) value);
        }
        if (value instanceof String) {
            return Entry.forCharacter((String) value);
        }
        if (value instanceof byte[]) {
            return Entry.forOctet((byte[]) value);
        }
        if (value instanceof boolean[]) {
            return Entry.forBit((boolean[]) value);
        }
        if (value instanceof LocalDate) {
            return Entry.forDate((LocalDate) value);
        }
        if (value instanceof LocalTime) {
            return Entry.forTimeOfDay((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return Entry.forTimePoint((LocalDateTime) value);
        }
        if (value instanceof LocalTime) {
            return Entry.forTimeOfDay((LocalTime) value);
        }
        if (value instanceof OffsetTime) {
            return Entry.forTimeOfDayWithTimeZone((OffsetTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return Entry.forTimePointWithTimeZone((OffsetDateTime) value);
        }
        if (value instanceof DateTimeInterval) {
            return Entry.forDateTimeInterval((DateTimeInterval) value);
        }
        return null;
    }

    /**
     * Creates a new instance for {@link EntryType#NULL}.
     * @return the created entry
     */
    public static Entry forNull() {
        return NULL;
    }

    /**
     * Returns whether or not this entry is {@link EntryType#NULL}.
     * @return true if it is such the type, false otherwise
     */
    public boolean isNullValue() {
        return getType() == EntryType.NULL;
    }

    /**
     * Creates a new instance for {@link EntryType#INT}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forInt(long value) {
        return new Entry(EntryType.INT, value);
    }

    /**
     * Returns as {@link EntryType#INT} value.
     * @return the value
     */
    public long getIntValue() {
        check(EntryType.INT);
        return (Long) value;
    }

    /**
     * Creates a new instance for {@link EntryType#FLOAT4}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forFloat4(float value) {
        return new Entry(EntryType.FLOAT4, value);
    }

    /**
     * Returns as {@link EntryType#INT} value.
     * @return the value
     */
    public float getFloat4Value() {
        check(EntryType.FLOAT4);
        return (Float) value;
    }

    /**
     * Creates a new instance for {@link EntryType#FLOAT8}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forFloat8(double value) {
        return new Entry(EntryType.FLOAT8, value);
    }

    /**
     * Returns as {@link EntryType#INT} value.
     * @return the value
     */
    public double getFloat8Value() {
        check(EntryType.FLOAT8);
        return (Double) value;
    }

    /**
     * Creates a new instance for {@link EntryType#DECIMAL}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forDecimal(BigDecimal value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.DECIMAL, value);
    }

    /**
     * Returns as {@link EntryType#DECIMAL} value.
     * @return the value
     */
    public BigDecimal getDecimalValue() {
        check(EntryType.DECIMAL);
        return (BigDecimal) value;
    }

    /**
     * Creates a new instance for {@link EntryType#CHARACTER}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forCharacter(String value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.CHARACTER, value);
    }

    /**
     * Returns as {@link EntryType#CHARACTER} value.
     * @return the value
     */
    public String getCharacterValue() {
        check(EntryType.CHARACTER);
        return (String) value;
    }

    /**
     * Creates a new instance for {@link EntryType#OCTET}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forOctet(byte[] value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.OCTET, value);
    }

    /**
     * Returns as {@link EntryType#OCTET} value.
     * @return the value
     */
    public byte[] getOctetValue() {
        check(EntryType.OCTET);
        return (byte[]) value;
    }

    /**
     * Creates a new instance for {@link EntryType#BIT}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forBit(boolean[] value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.BIT, value);
    }

    /**
     * Returns as {@link EntryType#BIT} value.
     * @return the value
     */
    public boolean[] getBitValue() {
        check(EntryType.BIT);
        return (boolean[]) value;
    }

    /**
     * Creates a new instance for {@link EntryType#DATE}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forDate(LocalDate value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.DATE, value);
    }

    /**
     * Returns as {@link EntryType#DATE} value.
     * @return the value
     */
    public LocalDate getDateValue() {
        check(EntryType.DATE);
        return (LocalDate) value;
    }

    /**
     * Creates a new instance for {@link EntryType#TIME_OF_DAY}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forTimeOfDay(LocalTime value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.TIME_OF_DAY, value);
    }

    /**
     * Returns as {@link EntryType#TIME_OF_DAY} value.
     * @return the value
     */
    public LocalTime getTimeOfDayValue() {
        check(EntryType.TIME_OF_DAY);
        return (LocalTime) value;
    }

    /**
     * Creates a new instance for {@link EntryType#TIME_POINT}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forTimePoint(LocalDateTime value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.TIME_POINT, value);
    }

    /**
     * Returns as {@link EntryType#TIME_POINT} value.
     * @return the value
     */
    public LocalDateTime getTimePointValue() {
        check(EntryType.TIME_POINT);
        return (LocalDateTime) value;
    }

    /**
     * Creates a new instance for {@link EntryType#TIME_OF_DAY_WITH_TIME_ZONE}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forTimeOfDayWithTimeZone(OffsetTime value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.TIME_OF_DAY_WITH_TIME_ZONE, value);
    }

    /**
     * Returns as {@link EntryType#TIME_OF_DAY} value.
     * @return the value
     */
    public OffsetTime getTimeOfDayWithTimeZoneValue() {
        check(EntryType.TIME_OF_DAY_WITH_TIME_ZONE);
        return (OffsetTime) value;
    }

    /**
     * Creates a new instance for {@link EntryType#TIME_POINT_WITH_TIME_ZONE}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forTimePointWithTimeZone(OffsetDateTime value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.TIME_POINT_WITH_TIME_ZONE, value);
    }

    /**
     * Returns as {@link EntryType#TIME_POINT_WITH_TIME_ZONE} value.
     * @return the value
     */
    public OffsetDateTime getTimePointWithTimeZoneValue() {
        check(EntryType.TIME_POINT_WITH_TIME_ZONE);
        return (OffsetDateTime) value;
    }

    /**
     * Creates a new instance for {@link EntryType#DATETIME_INTERVAL}.
     * @param value the value
     * @return the created entry
     */
    public static Entry forDateTimeInterval(DateTimeInterval value) {
        Objects.requireNonNull(value);
        return new Entry(EntryType.DATETIME_INTERVAL, value);
    }

    /**
     * Returns as {@link EntryType#DATETIME_INTERVAL} value.
     * @return the value
     */
    public DateTimeInterval getDateTimeIntervalValue() {
        check(EntryType.DATETIME_INTERVAL);
        return (DateTimeInterval) value;
    }

    /**
     * Creates a new instance for {@link EntryType#ARRAY}.
     * @param size the array size
     * @return the created entry
     */
    public static Entry forArray(int size) {
        return new Entry(EntryType.ARRAY, size);
    }

    /**
     * Returns as {@link EntryType#ARRAY} size.
     * @return the value
     */
    public int getArraySize() {
        check(EntryType.ARRAY);
        return (Integer) value;
    }

    /**
     * Creates a new instance for {@link EntryType#ROW}.
     * @param size the row size
     * @return the created entry
     */
    public static Entry forRow(int size) {
        return new Entry(EntryType.ROW, size);
    }

    /**
     * Returns as {@link EntryType#ROW} size.
     * @return the value
     */
    public int getRowSize() {
        check(EntryType.ROW);
        return (Integer) value;
    }

    // FIXME for clob, blob

    /**
     * Creates a new instance for {@link EntryType#END_OF_CONTENTS}.
     * @return the created entry
     */
    public static Entry forEndOfContents() {
        return END_OF_CONTENTS;
    }

    /**
     * Returns whether or not this entry is {@link EntryType#END_OF_CONTENTS}.
     * @return true if it is such the type, false otherwise
     */
    public boolean isEndOfContentsValue() {
        return getType() == EntryType.END_OF_CONTENTS;
    }

    private void check(EntryType required) {
        assert required != null;
        if (required != type) {
            throw new IllegalStateException(MessageFormat.format(
                    "inconsistent type: {0} was found, but required {1}",
                    type,
                    required));
        }
    }

    @Override
    public String toString() {
        return String.format("Entry(type=%s, value=%s)", type, value); //$NON-NLS-1$
    }
}
