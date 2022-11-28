package com.tsurugidb.tsubakuro.util;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Represents a resource which is on the server and needs request to dispose it for the server.
 */
public interface ServerResourceNeedingDisposal extends ServerResource {
    @Override
    default void setCloseTimeout(@Nullable Timeout t) {
        if (Objects.nonNull(t)) {
            setCloseTimeout(t.value(), t.unit());
        }
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    default void setCloseTimeout(long timeout, TimeUnit unit) {
        return;
    }
}
