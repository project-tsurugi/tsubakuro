package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.TimeUnit;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * PreparedStatement type.
 */
public interface PreparedStatement extends ServerResource {
    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    void setCloseTimeout(long timeout, TimeUnit unit);

    /**
     * Check whether ResultRecords are returned as a result of executing this statement
     * @return true if executing this statement returns ResultRecords
     */
    boolean hasResultRecords();
}
