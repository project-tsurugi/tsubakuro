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
 * Represents a behavior of {@code REMOVE} operation.
 */
public enum RemoveType {

    /**
     * Removes table entries with counting the actually removed entries (default behavior).
     * @see RemoveResult#size()
     */
    COUNTING,

    /**
     * Removes table entries without counting the removed entries.
     * <p>
     * Using this, {@link RemoveResult#size()} will return {@code 0}.
     * </p>
     */
    INSTANT,
    ;

    /**
     * The default behavior of {@code REMOVE} operation.
     * @see #COUNTING
     */
    public static final RemoveType DEFAULT_BEHAVIOR = COUNTING;
}
