package com.tsurugidb.tsubakuro.kvs;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;
import com.tsurugidb.kvs.proto.KvsData;

/**
 * A buffer to build a record.
 */
public class RecordBuffer {

    private final KvsData.Record.Builder entity;

    /**
     * Creates a new instance.
     */
    public RecordBuffer() {
        this.entity = KvsData.Record.newBuilder();
    }

    /**
     * Creates a new instance.
     * @param prototype the prototype record
     */
    public RecordBuffer(@Nonnull Record prototype) {
        Objects.requireNonNull(prototype);
        this.entity = KvsData.Record.newBuilder(prototype.getEntity());
    }

    /**
     * Returns the number of entries in this record.
     * @return the number of entries
     */
    public int size() {
        return entity.getValuesCount();
    }

    /**
     * Clears the added entries.
     * @return this
     */
    public RecordBuffer clear() {
        entity.clear();
        return this;
    }

    /**
     * Adds a new entry to this buffer with {@code NULL} value.
     *
     * @param name the entry name
     * @return this
     */
    public RecordBuffer addNull(@Nonnull String name) {
        Objects.requireNonNull(name);
        entity.addNames(name);
        entity.addValues(KvsData.Value.getDefaultInstance());
        return this;
    }

    /**
     * Adds a new entry to this buffer.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull KvsData.Value value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        entity.addNames(name);
        entity.addValues(value);
        return this;
    }

    /**
     * Adds a new entry to this buffer with {@code BOOL} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, boolean value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code INT} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, int value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code BIGINT} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, long value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code FLOAT} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, float value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code DOUBLE} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, double value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code DECIMAL} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull BigDecimal value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code CHAR, VARCHAR} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull String value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code BINARY, VARBINARY} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull byte[] value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code BINARY, VARBINARY} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull ByteString value) {
        return add(name, Values.ofBinary(value));
    }

    /**
     * Adds a new entry to this buffer with {@code DATE} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull LocalDate value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code TIME} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull LocalTime value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code TIMESTAMP} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull LocalDateTime value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code TIME WITH TIMEZONE} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull OffsetTime value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with {@code TIMESTAMP WITH TIMEZONE} value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull OffsetDateTime value) {
        return add(name, Values.of(value));
    }

    /**
     * Adds a new entry to this buffer with list value.
     *
     * @param name the entry name
     * @param value the entry value
     * @return this
     */
    public RecordBuffer add(@Nonnull String name, @Nonnull List<?> value) {
        return add(name, Values.of(value));
    }

    /**
     * Builds a record from this buffer.
     * @return the built record
     */
    public Record toRecord() {
        return new Record(entity.build());
    }

    @Override
    public String toString() {
        return String.valueOf(entity);
    }
}
