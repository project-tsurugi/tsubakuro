package com.tsurugidb.tsubakuro.auth;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.tsurugidb.auth.proto.AuthRequest;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

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
