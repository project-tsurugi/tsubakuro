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
            throw new IllegalArgumentException(MessageFormat.format(
                    "record entry count mismatch: names={0}, values={1}",
                    entity.getNamesCount(),
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
     * @return the value, or {@code null} if the value represents just a {@code NULL}
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @see #size()
     */
    public @Nullable Object getValue(int position) {
        var raw = entity.getValues(position);
        return Values.toObject(raw);
    }

    /**
     * Returns the value of the column of the name.
     * @param name name of the column
     * @return the value, or {@code null} if the value represents just a {@code NULL}
     * @throws IllegalArgumentException if the record doesn't have the name
     */
    public @Nullable Object getValue(@Nonnull String name) {
        Objects.requireNonNull(name);
        Integer index = name2posMap.get(name);
        if (index != null) {
            return getValue(index);
        }
        throw new IllegalArgumentException("unknown cloumn name: " + name);
    }

    private static String typeMismatched(String name, String type, Object o) {
        String s = name + " doesn't have " + type + " value";
        if (o != null) {
            s += ": " + o.getClass().getCanonicalName();
        }
        return s;
    }

    /**
     * Returns the boolean value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code boolean}
     */
    public boolean getBoolean(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof Boolean) {
            return (Boolean) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "boolean", o));
    }

    /**
     * Returns the integer value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code int}
     */
    public int getInt(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof Integer) {
            return (int) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "integer", o));
    }

    /**
     * Returns the long value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code long}
     */
    public long getLong(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof Long) {
            return (long) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "long", o));
    }

    /**
     * Returns the float value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code float}
     */
    public float getFloat(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof Float) {
            return (float) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "float", o));
    }

    /**
     * Returns the double value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code double}
     */
    public double getDouble(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof Double) {
            return (double) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "double", o));
    }

    /**
     * Returns the BigDecimal value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code BigDecimal}
     */
    public @Nonnull BigDecimal getDecimal(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof BigDecimal) {
            return (BigDecimal) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "BigDecimal", o));
    }

    /**
     * Returns the String value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code String}
     */
    public @Nonnull String getCharacter(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof String) {
            return (String) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "String", o));
    }

    /**
     * Returns the octet value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code byte[]}
     */
    public @Nonnull byte[] getOctet(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof byte[]) {
            return (byte[]) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "byte[]", o));
    }

    /**
     * Returns the date value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code LocalDate}
     */
    public @Nonnull LocalDate getDate(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof LocalDate) {
            return (LocalDate) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "LocalDate", o));
    }

    /**
     * Returns the time of day value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code LocalTime}
     */
    public @Nonnull LocalTime getTimeOfDay(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof LocalTime) {
            return (LocalTime) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "LocalTime", o));
    }

    /**
     * Returns the time point value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code LocalDateTime}
     */
    public @Nonnull LocalDateTime getTimePoint(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof LocalDateTime) {
            return (LocalDateTime) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "LocalDateTime", o));
    }

    /**
     * Returns the time of day with timezone value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code OffsetTime}
     */
    public @Nonnull OffsetTime getTimeOfDayWithTimeZone(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof OffsetTime) {
            return (OffsetTime) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "OffsetTime", o));
    }

    /**
     * Returns the time point with timezone value of the column of the specified name.
     * @param name name of the column
     * @return the value
     * @throws IllegalArgumentException if the record doesn't have the name or the value isn't {@code OffsetDateTime}
     */
    public @Nonnull OffsetDateTime getTimePointWithTimeZone(@Nonnull String name) {
        Object o = getValue(name);
        if (o instanceof OffsetDateTime) {
            return (OffsetDateTime) o;
        }
        throw new IllegalArgumentException(typeMismatched(name, "OffsetDateTime", o));
    }

    /**
     * Returns the name at the position.
     * @param position the entry position (0-origin)
     * @return the entry name: in most cases, it is a table column name
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @see #size()
     */
    public String getName(int position) {
        return entity.getNames(position);
    }

    /**
     * Returns the entity of this record.
     * @return the entity
     */
    public KvsData.Record getEntity() {
        return entity;
    }

    @Override
    public String toString() {
        return String.valueOf(entity);
    }
}
