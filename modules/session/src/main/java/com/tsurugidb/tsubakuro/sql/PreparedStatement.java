package com.tsurugidb.tsubakuro.sql;

import com.tsurugidb.tsubakuro.util.ServerResourceNeedingDisposal;

/**
 * PreparedStatement type.
 */
public interface PreparedStatement extends ServerResourceNeedingDisposal {
    /**
     * Check whether ResultRecords are returned as a result of executing this statement
     * @return true if executing this statement returns ResultRecords
     */
    boolean hasResultRecords();
}
