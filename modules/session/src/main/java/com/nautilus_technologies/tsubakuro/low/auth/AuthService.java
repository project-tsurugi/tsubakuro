package com.nautilus_technologies.tsubakuro.low.auth;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.tsurugidb.tateyama.proto.AuthRequest;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * An interface to communicate with datastore service.
 * @see AuthClient
 */
public interface AuthService extends ServerResource {

    /**
     * Requests {@code AuthInfo} to auth service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<AuthInfo> send(@Nonnull AuthRequest.AuthInfo request) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
