package com.tsurugidb.tsubakuro.auth;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.client.ServiceClient;
import com.tsurugidb.tsubakuro.client.ServiceMessageVersion;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * An auth service client.
 * @see #attach(Session)
 */
@ServiceMessageVersion(
        service = AuthClient.SERVICE,
        major = AuthClient.MAJOR,
        minor = AuthClient.MINOR)
public interface AuthClient extends ServerResource, ServiceClient {

    /**
     * the service name.
     */
    String SERVICE = "AUTH";

    /**
     * the major version.
     */
    int MAJOR = 1;

    /**
     * the minor version.
     */
    int MINOR = 0;

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    static AuthClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves authentication information.
     * @return the future response of authentication information,
     *      it will raise {@link AuthServiceException} if request was failure
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<AuthInfo> getAuthInfo() throws IOException {
        throw new UnsupportedOperationException();
    }
}
