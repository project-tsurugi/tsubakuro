package com.tsurugidb.tsubakuro.sql.impl;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.io.IOException;
import java.util.HashMap;

import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.CounterType;

/**
 * Represents an execution result of SQL statements.
 */
public class ExecuteResultAdapter implements ExecuteResult {
    private final HashSet<CounterType> counterTypes = new HashSet<>();
    private final HashMap<CounterType, Long> counters = new HashMap<>();

    ExecuteResultAdapter(SqlResponse.ExecuteResult.Success executeResult) throws IOException {
        var result = executeResult.getCountersList();
        for (var e : result) {
            var type = counterType(e.getType());
            counterTypes.add(type);
            counters.put(type, e.getValue());
        }
    }

    @Override
    public Set<CounterType> getCounterTypes() {
        return counterTypes;
    }

    @Override
    public Map<CounterType, Long> getCounters() {
        return counters;
    }

    private CounterType counterType(SqlResponse.ExecuteResult.CounterType type) throws IOException {
        switch (type) {
        case UPDATED_ROWS:
            return CounterType.UPDATED_ROWS;
        case DELETED_ROWS:
            return CounterType.DELETED_ROWS;
        }
        throw new IOException("illegal counter type");
    }
}
