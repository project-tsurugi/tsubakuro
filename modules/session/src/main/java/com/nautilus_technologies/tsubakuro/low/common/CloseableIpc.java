package com.nautilus_technologies.tsubakuro.low.common;

import java.util.concurrent.TimeUnit;
import java.io.Closeable;

/**
 * CloseableIpc type.
 */
public interface CloseableIpc extends Closeable {
    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    void setCloseTimeout(long t, TimeUnit u);
}
