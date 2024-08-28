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
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.auth.proto.AuthRequest;
import com.tsurugidb.tsubakuro.auth.AuthClient;
import com.tsurugidb.tsubakuro.auth.AuthInfo;
import com.tsurugidb.tsubakuro.auth.AuthService;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

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
