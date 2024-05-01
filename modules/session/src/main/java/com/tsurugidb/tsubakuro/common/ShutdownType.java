package com.tsurugidb.tsubakuro.common;

import com.tsurugidb.core.proto.CoreRequest;

public enum ShutdownType {
    /**
     * Waits for the ongoing requests and safely shutdown the session.
     * <p>
     * This may wait for complete the current running requests, and then shutdown this session.
     * </p>
     */
    GRACEFUL(CoreRequest.ShutdownType.GRACEFUL),

    /**
     * Cancelling the ongoing requests and safely shutdown the session.
     * <p>
     * This request will first the cancelling the ongoing requests.
     * Once each request detects the cancellation, it will discontinue the subsequent work.
     * Finally, this operation may wait for complete or cancel the requests, and then shutdown this session.
     * </p>
     */
    FORCEFUL(CoreRequest.ShutdownType.FORCEFUL);

    private final CoreRequest.ShutdownType type;

    ShutdownType(CoreRequest.ShutdownType type) {
        this.type = type;
    }

    public CoreRequest.ShutdownType type() {
        return type;
    }
}
