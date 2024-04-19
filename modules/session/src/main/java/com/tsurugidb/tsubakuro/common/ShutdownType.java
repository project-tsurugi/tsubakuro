package com.tsurugidb.tsubakuro.common;

import com.tsurugidb.endpoint.proto.EndpointRequest;

public enum ShutdownType {
    /**
     * Waits for the ongoing requests and safely shutdown the session.
     * <p>
     * This may wait for complete the current running requests, and then shutdown this session.
     * </p>
     */
    GRACEFUL(EndpointRequest.ShutdownType.GRACEFUL),

    /**
     * Cancelling the ongoing requests and safely shutdown the session.
     * <p>
     * This request will first the cancelling the ongoing requests.
     * Once each request detects the cancellation, it will discontinue the subsequent work.
     * Finally, this operation may wait for complete or cancel the requests, and then shutdown this session.
     * </p>
     */
    FORCEFUL(EndpointRequest.ShutdownType.FORCEFUL);

    private final EndpointRequest.ShutdownType type;

    ShutdownType(EndpointRequest.ShutdownType type) {
        this.type = type;
    }

    public EndpointRequest.ShutdownType type() {
        return type;
    }
}
