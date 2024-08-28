/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.auth.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.auth.proto.AuthRequest;
import com.tsurugidb.auth.proto.AuthResponse;
import com.tsurugidb.tsubakuro.auth.AuthClient;
import com.tsurugidb.tsubakuro.auth.AuthInfo;
import com.tsurugidb.tsubakuro.auth.AuthService;
import com.tsurugidb.tsubakuro.auth.AuthServiceCode;
import com.tsurugidb.tsubakuro.auth.AuthServiceException;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

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

    private static AuthRequest.Request.Builder newRequest() {
        return AuthRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(AuthClient.SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(AuthClient.SERVICE_MESSAGE_VERSION_MINOR);
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
            newRequest()
            .setAuthInfo(request)
            .build()
            .toByteArray(),
            new AuthInfoProcessor().asResponseProcessor());
    }
}
