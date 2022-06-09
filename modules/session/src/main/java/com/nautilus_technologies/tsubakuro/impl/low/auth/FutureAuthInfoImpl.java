package com.nautilus_technologies.tsubakuro.impl.low.auth;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.google.protobuf.Message;
import com.tsurugidb.tateyama.proto.AuthResponse;
import com.nautilus_technologies.tsubakuro.low.auth.AuthServiceCode;
import com.nautilus_technologies.tsubakuro.low.auth.AuthServiceException;
import com.nautilus_technologies.tsubakuro.channel.common.connection.RememberMeCredential;
import com.nautilus_technologies.tsubakuro.exception.BrokenResponseException;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.auth.AuthInfo;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.channel.common.FutureInputStream;

/**
 * FutureAuthInfoImpl type.
 */
public class FutureAuthInfoImpl implements FutureResponse<AuthInfo> {
    FutureInputStream futureInputStream;

    public FutureAuthInfoImpl(FutureInputStream futureInputStream) {
        this.futureInputStream = futureInputStream;
    }

    @Override
    public AuthInfo get() throws IOException, ServerException, InterruptedException {
        try {
            var message = AuthResponse.AuthInfo.parseDelimitedFrom(futureInputStream.get());

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
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: SessionWireImpl.receive()", e);
        }
    }

    @Override
    public AuthInfo get(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        // FIXME: impl
        return get();
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        // FIXME: impl
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
}
