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

/**
 * Represents a kind of execution counter of SQL statements.
 */
public enum CounterType {

    /**
     * The number of rows inserted in the execution.
     */
    INSERTED_ROWS,

    /**
     * The number of rows updated in the execution.
     */
    UPDATED_ROWS,

    /**
     * The number of rows inserted or replaced/updated or inserted in the execution.
     */
    MERGED_ROWS,

    /**
     * The number of rows deleted in the execution.
     */
    DELETED_ROWS,
}
