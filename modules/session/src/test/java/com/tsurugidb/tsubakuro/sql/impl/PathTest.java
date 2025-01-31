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

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlResponse;

class PathTest {
    private final SqlResponse.ListTables.Success listTables =
        SqlResponse.ListTables.Success.newBuilder()
            .addTablePathNames(SqlResponse.Name.newBuilder()
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("databaseName"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema1Name"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("table1Name")))
            .addTablePathNames(SqlResponse.Name.newBuilder()
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("databaseName"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema1Name"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("table2Name")))
            .addTablePathNames(SqlResponse.Name.newBuilder()
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("databaseName"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema2Name"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("table1Name")))
            .addTablePathNames(SqlResponse.Name.newBuilder()
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("databaseName"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema2Name"))
                                .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("table2Name")))
            .build();

    private final SqlResponse.GetSearchPath.Success searchPath =
        SqlResponse.GetSearchPath.Success.newBuilder()
            .addSearchPaths(SqlResponse.Name.newBuilder()
                            .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("databaseName"))
                            .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema1Name")))
            .addSearchPaths(SqlResponse.Name.newBuilder()
                            .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("databaseName"))
                            .addIdentifiers(SqlResponse.Identifier.newBuilder().setLabel("schema3Name")))
            .build();

    @Test
    void tableListAdapter() {
        HashSet expected = new HashSet<String>();
        expected.add("databaseName.schema1Name.table1Name");
        expected.add("databaseName.schema1Name.table2Name");
        expected.add("databaseName.schema2Name.table1Name");
        expected.add("databaseName.schema2Name.table2Name");

        var tableListAdapter = new TableListAdapter(listTables);
        for (var e: tableListAdapter.getTableNames()) {
            assertTrue(expected.remove(e));
        }
        assertEquals(expected.size(), 0);
    }

    @Test
    void searchPathAdapter() {
        HashSet expected = new HashSet<String>();
        expected.add("databaseName.schema1Name");
        expected.add("databaseName.schema3Name");

        var searchPathAdapter = new SearchPathAdapter(searchPath);
        for (var e: searchPathAdapter.getSchemaNames()) {
            assertTrue(expected.remove(e));
        }
        assertEquals(expected.size(), 0);
    }

    @Test
    void tableListAdapterWithsearchPathAdapter() {
        HashSet expected = new HashSet<String>();
        expected.add("databaseName.schema1Name.table1Name");
        expected.add("databaseName.schema1Name.table2Name");

        var tableListAdapter = new TableListAdapter(listTables);
        var searchPathAdapter = new SearchPathAdapter(searchPath);
        for (var e: tableListAdapter.getSimpleNames(searchPathAdapter)) {
            assertTrue(expected.remove(e));
        }
        assertEquals(expected.size(), 0);
    }
}
