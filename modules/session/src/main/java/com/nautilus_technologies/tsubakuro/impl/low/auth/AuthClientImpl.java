package com.nautilus_technologies.tsubakuro.impl.low.auth;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tateyama.proto.AuthRequest;
import com.nautilus_technologies.tsubakuro.low.auth.AuthClient;
import com.nautilus_technologies.tsubakuro.low.auth.AuthInfo;
import com.nautilus_technologies.tsubakuro.low.auth.AuthService;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Session;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link AuthClient}.
 */
public class AuthClientImpl implements AuthClient {

    private final AuthService service;

    /**
     * Attaches to the auth service in the current session.
     * @param session the current session
     * @return the auth service client
     */
    public static AuthClientImpl attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return new AuthClientImpl(new AuthServiceStub(session));
    }

    /**
     * Creates a new instance.
     * @param service the service stub
     */
    public AuthClientImpl(@Nonnull AuthService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public FutureResponse<AuthInfo> getAuthInfo() throws IOException {
        return service.send(AuthRequest.AuthInfo.getDefaultInstance());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        service.close();
    }
}
