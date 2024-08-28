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
package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a behavior of {@code PUT} operation.
 */
public enum PutType {

    /**
     * Puts a table entry in any case (default behavior).
     */
    OVERWRITE,

    /**
     * Puts a table entry only if it is absent.
     */
    IF_ABSENT,

    /**
     * Puts a table entry only if it is already exists.
     */
    IF_PRESENT,

    ;

    /**
     * The default behavior of {@code PUT} operation.
     * @see #OVERWRITE
     */
    public static final PutType DEFAULT_BEHAVIOR = OVERWRITE;
}
