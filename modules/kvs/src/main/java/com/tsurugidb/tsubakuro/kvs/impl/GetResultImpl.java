package com.tsurugidb.tsubakuro.kvs.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.Record;

/**
 * An implementation of {@link GetResult}.
 */
public class GetResultImpl implements GetResult {

    private final List<Record> records = new LinkedList<>();

    /**
     * Creates a new instance.
     * @param record the retrieved record
     */
    public GetResultImpl(@Nonnull KvsData.Record record) {
        Objects.requireNonNull(record);
        this.records.add(new Record(record));
    }

    /**
     * Creates a new instance.
     * @param records list of retrieved records
     */
    public GetResultImpl(@Nonnull List<KvsData.Record> records) {
        Objects.requireNonNull(records);
        // since Java 16
        // this.records.addAll(records.stream().map(r -> new Record(r)).toList());
        for (var r : records) {
            this.records.add(new Record(r));
        }
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public Optional<? extends Record> asOptional() {
        switch (size()) {
        case 0:
            return Optional.empty();
        case 1:
            return Optional.of(records.get(0));
        default:
            throw new IllegalStateException("result has " + size() + " records");
        }
    }

    @Override
    public Record asRecord() {
        if (size() == 1) {
            return records.get(0);
        } else {
            throw new IllegalStateException("result has " + size() + " records");
        }
    }

    @Override
    public List<? extends Record> asList() {
        return records;
    }

}
