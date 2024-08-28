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

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.HashMap;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.kvs.proto.KvsData.Value.ValueCase;

/**
 * Represents a record of values.
 */
public class Record {

    private final KvsData.Record entity;
    private final HashMap<String, Integer> name2posMap = new HashMap<>();

    /**
     * Creates a new empty instance.
     */
    public Record() {
        this(KvsData.Record.getDefaultInstance());
    }

    /**
     * Creates a new instance.
     * @param entity the wrapped entity
     * @throws IllegalArgumentException if record names and values are mismatch
     */
    public Record(@Nonnull KvsData.Record entity) {
        Objects.requireNonNull(entity);
        if (entity.getNamesCount() != entity.getValuesCount()) {
            throw new IllegalArgumentException(
                    MessageFormat.format("record entry count mismatch: names={0}, values={1}", entity.getNamesCount(),
                            entity.getValuesCount()));
        }
        this.entity = entity;
        for (int position = 0; position < entity.getNamesCount(); position++) {
            name2posMap.put(entity.getNames(position), position);
        }
    }

    /**
     * Returns the number of entries in this record.
     * @return the number of entries
     */
    public int size() {
        return entity.getValuesCount();
    }

    /**
     * Returns the value at the position.
     * @param position the entry position (0-origin)
     * @return the object contains value type and value object
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @see #size()
     */
    private @Nonnull KvsData.Value getKvsDataValue(int position) {
        return entity.getValues(position);
    }

    /**
     * Returns the value at the position.
     * @param position the entry position (0-origin)
     * @return the value, or {@code null} if the value represents just a
     *         {@code NULL}
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @see #size()
     */
    public @Nullable Object getValue(int position) {
        return Values.toObject(getKvsDataValue(position));
    }

    /**
     * Returns the value of the column of the name.
     * @param name name of the column
     * @return the object contains value type and value object
     * @throws IllegalArgumentException if the record doesn't have the name
     */
    private @Nonnull KvsData.Value getKvsDataValue(@Nonnull String name) {
        Objects.requireNonNull(name);
        Integer index = name2posMap.get(name);
        if (index != null) {
            return getKvsDataValue(index);
        }
        throw new IllegalArgumentException(MessageFormat.format("unknown cloumn name: {0}", name));
    }

    /**
     * Returns the value of the column of the name.
     * @param name name of the column
     * @return the value, or {@code null} if the value represents just a
     *         {@code NULL}
     * @throws IllegalArgumentException if the record doesn't have the name
     */
    public @Nullable Object getValue(@Nonnull String name) {
        Objects.requireNonNull(name);
        Integer index = name2posMap.get(name);
        if (index != null) {
            return getValue(index);
        }
        throw new IllegalArgumentException(MessageFormat.format("unknown cloumn name: {0}", name));
    }

    /**
     * Returns whether the value of the column of the name is null.
     * @param name name of the column
     * @return true if the value is null, otherwise false
     * @throws IllegalArgumentException if the record doesn't have the name
     */
    public boolean isNull(@Nonnull String name) {
        var value = getKvsDataValue(name);
        return value.getValueCase() == ValueCase.VALUE_NOT_SET;
    }

    private static String valueCase2name(KvsData.Value.ValueCase vc) {
        switch (vc) {
        case BOOLEAN_VALUE:
            return "BOOL";
        case INT4_VALUE:
            return "INT";
        case INT8_VALUE:
            return "BIGINT";
        case FLOAT4_VALUE:
            return "FLOAT";
        case FLOAT8_VALUE:
            return "DOUBLE";
        case DECIMAL_VALUE:
            return "DECIMAL";
        case CHARACTER_VALUE:
            return "CHAR/VARCHAR";
        case OCTET_VALUE:
            return "BINARY/VARBINARY";
        case DATE_VALUE:
            return "DATE";
        case TIME_OF_DAY_VALUE:
            return "TIME";
        case TIME_POINT_VALUE:
            return "TIMESTAMP";
        case TIME_OF_DAY_WITH_TIME_ZONE_VALUE:
            return "TIME WITH TIMEZONE";
        case TIME_POINT_WITH_TIME_ZONE_VALUE:
            return "TIMESTAMP WITH TIMEZONE";
        case LIST_VALUE:
            return "LIST";
        case RECORD_VALUE:
            return "RECORD";
        case VALUE_NOT_SET:
            return "VALUE_NOT_SET";
        case DATETIME_INTERVAL_VALUE:
            return "DATETIME_INTERVAL"; // TODO not supported
        default:
            return vc.name();
        }
    }

    private static String typeMismatched(String name, ValueCase type, KvsData.Value value) {
        // NOTE <''> uses for showing <'> in MessageFormat
        return MessageFormat.format("{0} doesn''t have {1} value, it has {2}", name, valueCase2name(type),
                valueCase2name(value.getValueCase()));
    }

    /**
     * Returns the boolean value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code BOOL}
     */
    public boolean getBoolean(@Nonnull String name) {
        final ValueCase vc = ValueCase.BOOLEAN_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return value.getBooleanValue();
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the integer value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code INT}
     */
    public int getInt(@Nonnull String name) {
        final ValueCase vc = ValueCase.INT4_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return value.getInt4Value();
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the long value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code BIGINT}
     */
    public long getLong(@Nonnull String name) {
        final ValueCase vc = ValueCase.INT8_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return value.getInt8Value();
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the float value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code FLOAT}
     */
    public float getFloat(@Nonnull String name) {
        final ValueCase vc = ValueCase.FLOAT4_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return value.getFloat4Value();
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the double value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code DOUBLE}
     */
    public double getDouble(@Nonnull String name) {
        final ValueCase vc = ValueCase.FLOAT8_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return value.getFloat8Value();
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the BigDecimal value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code DECIMAL}
     */
    public @Nonnull BigDecimal getDecimal(@Nonnull String name) {
        final ValueCase vc = ValueCase.DECIMAL_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return Values.toObject(value.getDecimalValue());
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the String value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code CHAR} nor
     *         {@code VARCHAR}
     */
    public @Nonnull String getCharacter(@Nonnull String name) {
        final ValueCase vc = ValueCase.CHARACTER_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return value.getCharacterValue();
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the octet value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code BINARY} or
     *         {@code VARBINARY}
     */
    public @Nonnull byte[] getOctet(@Nonnull String name) {
        final ValueCase vc = ValueCase.OCTET_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return Values.toObject(value.getOctetValue());
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the date value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code DATE}
     */
    public @Nonnull LocalDate getDate(@Nonnull String name) {
        final ValueCase vc = ValueCase.DATE_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return LocalDate.ofEpochDay(value.getDateValue());
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the time of day value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code TIME}
     */
    public @Nonnull LocalTime getTimeOfDay(@Nonnull String name) {
        final ValueCase vc = ValueCase.TIME_OF_DAY_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return LocalTime.ofNanoOfDay(value.getTimeOfDayValue());
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the time point value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not {@code TIMEPOINT}
     */
    public @Nonnull LocalDateTime getTimePoint(@Nonnull String name) {
        final ValueCase vc = ValueCase.TIME_POINT_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return Values.toObject(value.getTimePointValue());
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the time of day with timezone value of the column of the specified
     * name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not
     *         {@code TIME WITH TIMEZONE}
     */
    public @Nonnull OffsetTime getTimeOfDayWithTimeZone(@Nonnull String name) {
        final ValueCase vc = ValueCase.TIME_OF_DAY_WITH_TIME_ZONE_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return Values.toObject(value.getTimeOfDayWithTimeZoneValue());
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the time point with timezone value of the column of the specified
     * name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException the record doesn't have the column of the
     *         name
     * @throws IllegalArgumentException the value is {@code NULL}
     * @throws IllegalArgumentException the value type is not
     *         {@code TIMEPOINT WITH TIMEZONE}
     */
    public @Nonnull OffsetDateTime getTimePointWithTimeZone(@Nonnull String name) {
        final ValueCase vc = ValueCase.TIME_POINT_WITH_TIME_ZONE_VALUE;
        var value = getKvsDataValue(name);
        if (value.getValueCase() == vc) {
            return Values.toObject(value.getTimePointWithTimeZoneValue());
        }
        throw new IllegalArgumentException(typeMismatched(name, vc, value));
    }

    /**
     * Returns the name at the position.
     * @param position the entry position (0-origin)
     * @return the entry name: in most cases, it is a table column name
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @see #size()
     */
    public @Nonnull String getName(int position) {
        return entity.getNames(position);
    }

    /**
     * Returns the entity of this record.
     * @return the entity
     */
    public @Nonnull KvsData.Record getEntity() {
        return entity;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Record)) {
            return false;
        }
        var other = (Record) o;
        return this.entity.equals(other.entity);
    }

    @Override
    public int hashCode() {
        return entity.hashCode();
    }

    @Override
    public String toString() {
        return String.valueOf(entity);
    }
}
