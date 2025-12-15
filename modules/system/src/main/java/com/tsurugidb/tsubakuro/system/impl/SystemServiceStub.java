/*
 * Copyright 2025-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.system.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.system.proto.SystemRequest;
import com.tsurugidb.system.proto.SystemResponse;
import com.tsurugidb.tsubakuro.system.SystemClient;
import com.tsurugidb.tsubakuro.system.SystemService;
import com.tsurugidb.tsubakuro.system.SystemServiceCode;
import com.tsurugidb.tsubakuro.system.SystemServiceException;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * An implementation of {@link SystemService} communicate to the system service.
 */
public class SystemServiceStub implements SystemService {

    static final Logger LOG = LoggerFactory.getLogger(SystemServiceStub.class);

    /**
     * The system service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_SYSTEM;

    private final Session session;

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public SystemServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
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

    private static SystemRequest.Request.Builder newRequest() {
        return SystemRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(SystemClient.SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(SystemClient.SERVICE_MESSAGE_VERSION_MINOR);
    }

    static class GetSystemInfoProcessor implements MainResponseProcessor<SystemResponse.SystemInfo> {
        @Override
        public SystemResponse.SystemInfo process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = SystemResponse.GetSystemInfo.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var res = message.getSuccess();
                return res.getSystemInfo();

            case ERROR:
                var err = message.getError();
                switch (err.getCode()) {
                case ERROR_CODE_NOT_SPECIFIED:
                    throw new BrokenResponseException("error.code is not set"); //$NON-NLS-1$
                case UNKNOWN:
                    throw new SystemServiceException(SystemServiceCode.UNKNOWN, err.getMessage());
                case NOT_FOUND:
                    throw new SystemServiceException(SystemServiceCode.NOT_FOUND, err.getMessage());
                default:
                    throw new BrokenResponseException(
                        MessageFormat.format(
                            "unknown error code: {0} ({1})",
                            err.getCode(),
                            err.getMessage()
                        )
                    );
                }

            case RESULT_NOT_SET:
                throw newResultNotSet(SystemResponse.GetSystemInfo.class, "result");

            default:
                throw new AssertionError("Unknown result case: " + message.getResultCase());
            }
        }
    }

    @Override
    public FutureResponse<SystemResponse.SystemInfo> send(SystemRequest.GetSystemInfo request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
            SERVICE_ID,
            toDelimitedByteArray(
                newRequest()
                .setGetSystemInfo(request)
                .build()),
            new GetSystemInfoProcessor().asResponseProcessor());
    }

    private static byte[] toDelimitedByteArray(Message request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }
}
