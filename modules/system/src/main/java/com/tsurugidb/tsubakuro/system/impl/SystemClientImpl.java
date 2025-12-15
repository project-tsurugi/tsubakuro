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

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.system.proto.SystemRequest;
import com.tsurugidb.system.proto.SystemResponse;
import com.tsurugidb.tsubakuro.system.SystemClient;
import com.tsurugidb.tsubakuro.system.SystemService;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link SystemClient}.
 */
public class SystemClientImpl implements SystemClient {

    private final SystemService service;

    /**
     * Attaches to the system service in the current session.
     * @param session the current session
     * @return the system service client
     */
    public static SystemClientImpl attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return new SystemClientImpl(new SystemServiceStub(session));
    }

    /**
     * Creates a new instance.
     * @param service the service stub
     */
    public SystemClientImpl(@Nonnull SystemService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public FutureResponse<SystemResponse.SystemInfo> getSystemInfo() throws IOException {
        return service.send(SystemRequest.GetSystemInfo.getDefaultInstance());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        service.close();
    }
}
