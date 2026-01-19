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

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;

/**
 * Represents an execution result of SQL statements.
 */
public class ExecuteResultAdapter implements ExecuteResult {
    private final Map<CounterType, Long> counters = new EnumMap<>(CounterType.class);

    ExecuteResultAdapter() {
    }

    ExecuteResultAdapter(SqlResponse.ExecuteResult.Success executeResult) throws IOException {
        var result = executeResult.getCountersList();
        for (var e : result) {
            var type = counterType(e.getType());
            if (type != null) {
                counters.put(type, e.getValue());
            }
        }
    }

    @Override
    public Map<CounterType, Long> getCounters() {
        return counters;
    }

    private CounterType counterType(SqlResponse.ExecuteResult.CounterType type) throws IOException {
        switch (type) {
        case INSERTED_ROWS:
            return CounterType.INSERTED_ROWS;
        case UPDATED_ROWS:
            return CounterType.UPDATED_ROWS;
        case  MERGED_ROWS:
            return CounterType.MERGED_ROWS;
        case DELETED_ROWS:
            return CounterType.DELETED_ROWS;
        default:
            break;
        }
        return null;
    }
}
