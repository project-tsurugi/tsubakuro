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
        service = AuthClient.SERVICE_SYMBOLIC_ID,
        major = AuthClient.SERVICE_MESSAGE_VERSION_MAJOR,
        minor = AuthClient.SERVICE_MESSAGE_VERSION_MINOR)
public interface AuthClient extends ServerResource, ServiceClient {

    /**
     * The symbolic ID of the destination service.
    */
    String SERVICE_SYMBOLIC_ID = "auth";

    /**
     * The major service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MINOR = 0;

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
