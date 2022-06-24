package com.nautilus_technologies.tsubakuro.low.auth;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Session;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * An auth service client.
 * @see #attach(Session)
 */
public interface AuthClient extends ServerResource {

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
