package com.nautilus_technologies.tsubakuro.impl.low.auth;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.tateyama.proto.AuthRequest;
import com.tsurugidb.tateyama.proto.AuthResponse;
import com.nautilus_technologies.tsubakuro.low.auth.AuthInfo;
import com.nautilus_technologies.tsubakuro.low.auth.AuthService;
import com.nautilus_technologies.tsubakuro.low.auth.AuthServiceCode;
import com.nautilus_technologies.tsubakuro.low.auth.AuthServiceException;
import com.nautilus_technologies.tsubakuro.channel.common.connection.RememberMeCredential;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.channel.common.wire.MainResponseProcessor;
import com.nautilus_technologies.tsubakuro.exception.BrokenResponseException;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;

/**
 * An implementation of {@link AuthService} communicate to the auth service.
 */
public class AuthServiceStub implements AuthService {

    static final Logger LOG = LoggerFactory.getLogger(AuthServiceStub.class);

    /**
     * The datastore service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_AUTH;

    private final Session session;

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public AuthServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    static AuthServiceException newUnknown(@Nonnull AuthResponse.UnknownError message) {
        assert message != null;
        return new AuthServiceException(AuthServiceCode.UNKNOWN, message.getMessage());
    }

    static BrokenResponseException newResultNotSet(
            @Nonnull Class<? extends Message> aClass, @Nonnull String name) {
        assert aClass != null;
        assert name != null;
        return new BrokenResponseException(MessageFormat.format(
                "{0}.{1} is not set",
                aClass.getSimpleName(),
                name));
    }

    static class AuthInfoProcessor implements MainResponseProcessor<AuthInfo> {
        @Override
        public AuthInfo process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = AuthResponse.AuthInfo.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var res = message.getSuccess();
                return new AuthInfo(res.getUser(), new RememberMeCredential(res.getToken()));

            case NOT_AUTHENTICATED:
                throw new AuthServiceException(AuthServiceCode.NOT_AUTHENTICATED);

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<AuthInfo> send(AuthRequest.AuthInfo request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
            SERVICE_ID,
            AuthRequest.Request.newBuilder()
            .setAuthInfo(request)
            .build()
            .toByteArray(),
            new AuthInfoProcessor().asResponseProcessor());
    }
}
