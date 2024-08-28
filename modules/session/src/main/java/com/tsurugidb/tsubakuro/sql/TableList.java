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
package com.tsurugidb.tsubakuro.sql;

import java.util.List;

/**
 * Represents table list.
 */
public interface TableList {

    /**
     * Returns a list of the available table names in the database, except system tables.
     * @return a list of the available table names
     */
    List<String> getTableNames();

    /**
     * Returns the schema name where the table defined.
     * @param searchPath the search path
     * @return a list of SimpleNames
     */
    List<String> getSimpleNames(SearchPath searchPath);
}
