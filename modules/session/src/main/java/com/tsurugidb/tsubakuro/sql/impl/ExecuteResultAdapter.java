package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.text.MessageFormat;
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
            counters.put(type, e.getValue());
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
        }
        throw new IOException(MessageFormat.format("illegal counter type. type={0}", type));
    }
}
