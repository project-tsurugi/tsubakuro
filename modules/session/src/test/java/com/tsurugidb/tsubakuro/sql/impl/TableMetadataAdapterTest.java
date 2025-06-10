/*
 * Copyright 2023-2025 Project Tsurugi.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlResponse.DescribeTable;

class TableMetadataAdapterTest {
        
    @Test
    void full() {
        DescribeTable.Success proto = DescribeTable.Success.newBuilder()
                .setDatabaseName("db")
                .setSchemaName("schema")
                .setTableName("table")
                .setDescription("description")
                .addPrimaryKey("pk1")
                .addPrimaryKey("pk2")
                .build();
        var table = new TableMetadataAdapter(proto);
        assertEquals("db", table.getDatabaseName().get());
        assertEquals("schema", table.getSchemaName().get());
        assertEquals("table", table.getTableName());
        assertEquals("description", table.getDescription().get());
        assertEquals("pk1", table.getPrimaryKeys().get(0));
        assertEquals("pk2", table.getPrimaryKeys().get(1));
        assertEquals(2, table.getPrimaryKeys().size());
    }

    @Test
    void emptyString() {
        DescribeTable.Success proto = DescribeTable.Success.newBuilder()
                .setDatabaseName("")
                .setSchemaName("")
                .setTableName("table")
                .setDescription("")
                .build();
        var table = new TableMetadataAdapter(proto);
        assertEquals("table", table.getTableName());
        assertThrows(NoSuchElementException.class, () -> table.getDatabaseName().orElseThrow());
        assertThrows(NoSuchElementException.class, () -> table.getSchemaName().orElseThrow());
        assertThrows(NoSuchElementException.class, () -> table.getDescription().orElseThrow());
    }

    @Test
    void empty() {
        DescribeTable.Success proto = DescribeTable.Success.newBuilder()
                .setTableName("table")
                .build();
        var table = new TableMetadataAdapter(proto);
        assertEquals("table", table.getTableName());
        assertThrows(NoSuchElementException.class, () -> table.getDatabaseName().orElseThrow());
        assertThrows(NoSuchElementException.class, () -> table.getSchemaName().orElseThrow());
        assertThrows(NoSuchElementException.class, () -> table.getDescription().orElseThrow());
    }
}