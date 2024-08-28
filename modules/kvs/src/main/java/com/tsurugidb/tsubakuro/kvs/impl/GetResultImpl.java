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
package com.tsurugidb.tsubakuro.kvs.impl;

import java.util.ArrayList;
import java.util.Collections;
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

    private final List<Record> records;

    /**
     * Creates a new instance.
     * @param record the retrieved record
     */
    public GetResultImpl(@Nonnull KvsData.Record record) {
        Objects.requireNonNull(record);
        this.records = new ArrayList<Record>(1);
        this.records.add(new Record(record));
    }

    /**
     * Creates a new instance.
     * @param records list of retrieved records
     */
    public GetResultImpl(@Nonnull List<KvsData.Record> records) {
        Objects.requireNonNull(records);
        this.records = new ArrayList<Record>(records.size());
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
        return Collections.unmodifiableList(records);
    }

}
