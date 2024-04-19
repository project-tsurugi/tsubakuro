package com.tsurugidb.tsubakuro.common.impl;

/**
 * The constant values.
 */
public final class Constants {

    /**
     * The service ID of core service.
     */
    public static final int SERVICE_ID_CORE = 0;

    /**
     * The major service message version for EndpointRequest.
     */
    public static final int ENDPOINT_BROKER_SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version for EndpointRequest.
     */
    public static final int ENDPOINT_BROKER_SERVICE_MESSAGE_VERSION_MINOR = 0;

    /**
     * The service id for endpoint broker.
     */
    public static final int SERVICE_ID_ENDPOINT_BROKER = 1;

    private Constants() {
        throw new AssertionError();
    }
}
