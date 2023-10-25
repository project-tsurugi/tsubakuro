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
