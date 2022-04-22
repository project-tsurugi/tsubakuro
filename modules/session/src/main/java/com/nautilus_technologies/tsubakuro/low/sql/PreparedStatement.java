package com.nautilus_technologies.tsubakuro.low.sql;

import com.nautilus_technologies.tsubakuro.low.common.CloseableIpc;

/**
 * PreparedStatement type.
 */
public interface PreparedStatement extends CloseableIpc {
    /**
     * Check whether ResultRecords are returned as a result of executing this statement
     * @return true if executing this statement returns ResultRecords
     */
    boolean hasResultRecords();
}
