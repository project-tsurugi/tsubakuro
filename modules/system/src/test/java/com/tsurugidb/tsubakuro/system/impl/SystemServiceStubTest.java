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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.diagnostics.proto.Diagnostics;
import com.tsurugidb.framework.proto.FrameworkCommon;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.system.proto.SystemRequest;
import com.tsurugidb.system.proto.SystemResponse;
import com.tsurugidb.system.proto.SystemDiagnostic;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.FileBlobInfo;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.system.SystemServiceCode;
import com.tsurugidb.tsubakuro.system.SystemServiceException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class SystemServiceStubTest {

    private final MockLink link = new MockLink();

    private WireImpl wire = null;

    private Session session = null;

    private Disposer disposer = null;

    private String testName = "testdb";
    private String testVersion = "1.1.1";
    private String testDate = "2025-12-24T01:23Z";
    private String testUrl = "//tree/01234567890123456789abcdefhijklmnopqrstu";

    SystemServiceStubTest() {
        try {
            wire = new WireImpl(link);
            session = new SessionImpl();
            disposer = ((SessionImpl) session).disposer();
            session.connect(wire);
     } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        if (session != null) {
            session.close();
        }
    }

    @Test
    void sendGetSystemInfoSuccess() throws Exception {
        link.next(SystemResponse.GetSystemInfo.newBuilder()
                    .setSuccess(SystemResponse.GetSystemInfo.Success.newBuilder()
                                    .setSystemInfo(SystemResponse.SystemInfo.newBuilder()
                                                        .setName(testName)
                                                        .setVersion(testVersion)
                                                        .setDate(testDate)
                                                        .setUrl(testUrl)))
                    .build()
        );

        var message = SystemRequest.GetSystemInfo.newBuilder().build();
        try (
            var service = new SystemServiceStub(session);
            var future = service.send(message)
        ) {
            var systemInfo = future.await();
            assertEquals(testName, systemInfo.getName());
            assertEquals(testVersion, systemInfo.getVersion());
            assertEquals(testDate, systemInfo.getDate());
            assertEquals(testUrl, systemInfo.getUrl());
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void sendGetDatabaseProductApplicationError() throws Exception {
        link.next(SystemResponse.GetSystemInfo.newBuilder()
                    .setError(SystemResponse.Error.newBuilder()
                                .setCode(SystemDiagnostic.ErrorCode.NOT_FOUND)
                                .setMessage("Not Found"))
                    .build()
        );

        var message = SystemRequest.GetSystemInfo.newBuilder().build();
        try (
            var service = new SystemServiceStub(session);
            var future = service.send(message)
        ) {
            var e = assertThrows(SystemServiceException.class, () -> future.await());
            assertEquals(SystemServiceCode.NOT_FOUND, e.getDiagnosticCode());
            assertTrue(e.getMessage().contains("Not Found"));
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void sendGetSystemInfoCoreError() throws Exception {
        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS)
                        .build();
        var payload = Diagnostics.Record.newBuilder()
                        .setCode(Diagnostics.Code.INVALID_REQUEST)
                        .setMessage("Core Error")
                        .build();
        link.next(header, payload);

        var message = SystemRequest.GetSystemInfo.newBuilder().build();
        try (
            var service = new SystemServiceStub(session);
            var future = service.send(message)
        ) {
            var e = assertThrows(CoreServiceException.class, () -> future.await());
            assertEquals(CoreServiceCode.INVALID_REQUEST, e.getDiagnosticCode());
            assertTrue(e.getMessage().contains("Core Error"));
        }
        assertFalse(link.hasRemaining());
    }
}
