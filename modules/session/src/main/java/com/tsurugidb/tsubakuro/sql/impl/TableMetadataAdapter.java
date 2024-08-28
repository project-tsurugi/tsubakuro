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
package com.tsurugidb.tsubakuro.sql.impl;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlResponse;

/**
 * An implementation of {@link TableMetadata} which just wraps original protocol buffers' message.
 */
public class TableMetadataAdapter implements TableMetadata {

    private final SqlResponse.DescribeTable.Success proto;

    /**
     * Creates a new instance.
     * @param proto the corresponding protocol buffers message
     */
    public TableMetadataAdapter(@Nonnull SqlResponse.DescribeTable.Success proto) {
        Objects.requireNonNull(proto);
        this.proto = proto;
    }

    @Override
    public Optional<String> getDatabaseName() {
        return Optional.of(proto.getDatabaseName())
                .filter(it -> !it.isEmpty());
    }

    @Override
    public Optional<String> getSchemaName() {
        return Optional.of(proto.getSchemaName())
                .filter(it -> !it.isEmpty());
    }

    @Override
    public String getTableName() {
        return proto.getTableName();
    }

    @Override
    public List<? extends SqlCommon.Column> getColumns() {
        return proto.getColumnsList();
    }

    @Override
    public String toString() {
        return proto.toString();
    }
}
