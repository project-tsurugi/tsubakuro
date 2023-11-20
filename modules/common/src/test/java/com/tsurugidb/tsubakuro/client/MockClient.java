package com.tsurugidb.tsubakuro.client;

/**
 * An implementation of {@link ServiceClient} for testing.
 */
@ServiceMessageVersion(
        service = MockClient.SERVICE,
        major = MockClient.MAJOR,
        minor = MockClient.MINOR)
public interface MockClient extends ServiceClient {

    /**
     * the service name.
     */
    String SERVICE = "MOCK";

    /**
     * the major version.
     */
    int MAJOR = 100;

    /**
     * the minor version.
     */
    int MINOR = 200;
}
