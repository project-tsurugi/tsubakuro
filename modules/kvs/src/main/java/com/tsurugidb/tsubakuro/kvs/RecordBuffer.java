package com.tsurugidb.tsubakuro.kvs;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
     * Adds a new entry to this buffer.
     * <p>
     * This never checks whether or not
     * </p>
     * @param name the entry name
     * @param value the entry value, or {@code null} if it represents {@code NULL}
     * @return this
     * @throws IllegalArgumentException if the input value is not supported for the record entry
     */
    public RecordBuffer add(@Nonnull String name, @Nullable Object value) {
        Objects.requireNonNull(name);
        entity.addNames(name);
        entity.addValues(Values.toValue(value));
        return this;
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
