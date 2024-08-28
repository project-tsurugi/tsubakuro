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
package com.tsurugidb.tsubakuro.sql.io;

/**
 * Represents a data type.
 */
public enum EntryType {

    /**
     * Pseudo data type of end of relation mark.
     */
    END_OF_CONTENTS,

    /**
     * Represents value is absent.
     */
    NULL,

    /**
     * 64-bit signed integer.
     * This represents {@code INT4}, {@code INT8} or {@code BOOLEAN}.
     */
    INT,

    /**
     * Fixed 32-bit floating point number.
     */
    FLOAT4,

    /**
     * Fixed 64-bit floating point number.
     */
    FLOAT8,

    /**
     * Fixed 128-bit floating point decimal.
     */
    DECIMAL,

    /**
     * Variable length character sequence.
     */
    CHARACTER,

    /**
     * Variable length octet sequence.
     */
    OCTET,

    /**
     * Variable length bit sequence.
     */
    BIT,

    /**
     * Date value.
     */
    DATE,

    /**
     * Time of day value.
     */
    TIME_OF_DAY,

    /**
     * Time-point value.
     */
    TIME_POINT,

    /**
     * Time of day with timezone value.
     */
    TIME_OF_DAY_WITH_TIME_ZONE,

    /**
     * Time-point with timezone value.
     */
    TIME_POINT_WITH_TIME_ZONE,

    /**
     * Date-time interval value.
     */
    DATETIME_INTERVAL,

    /**
     * Rows.
     */
    ROW,

    /**
     * Arrays.
     */
    ARRAY,

    /**
     * Character large objects.
     */
    CLOB,

    /**
     * Binary large objects.
     */
    BLOB,
}
