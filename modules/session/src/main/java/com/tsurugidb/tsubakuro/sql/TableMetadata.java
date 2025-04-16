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

import java.util.Optional;

/**
 * Represents metadata of tables.
 */
public interface TableMetadata extends RelationMetadata {

    /**
     * <em>This method is not yet implemented:</em>
     * Returns the database name where the table defined.
     * @return the database name, or empty if it is not set
     */
    Optional<String> getDatabaseName();

    /**
     * <em>This method is not yet implemented:</em>
     * Returns the schema name where the table defined.
     * @return the schema name, or empty if it is not set
     */
    Optional<String> getSchemaName();

    /**
     * Returns simple name of the table.
     * @return the simple name
     */
    String getTableName();

    /**
     * Returns description of tha table.
     * @return the description of tha table, or empty if it is not set
     */
    Optional<String> getDescription();
}
