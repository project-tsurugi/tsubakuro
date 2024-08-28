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

import java.util.Map;
import java.util.Set;

/**
 * Represents an execution result of SQL statements.
 */
public interface ExecuteResult {

    /**
     * Returns the all available counter types in this result.
     * @return the available counter types
     */
    default Set<CounterType> getCounterTypes() {
        return getCounters().keySet();
    }

    /**
     * Returns the all available counter entries in this result.
     * @return the available counter entries
     */
    default Map<CounterType, Long> getCounters() {
        throw new UnsupportedOperationException();
    }
}
