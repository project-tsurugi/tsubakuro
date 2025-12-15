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

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.system.proto.SystemRequest;
import com.tsurugidb.system.proto.SystemResponse;
import com.tsurugidb.tsubakuro.system.SystemService;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class SystemClientImplTest {
    private String testName = "testdb";
    private String testVersion = "1.1.1";
    private String testDate = "2025-12-24T01:23Z";
    private String testUrl = "//tree/01234567890123456789abcdefhijklmnopqrstu";

    @Test
    void getSystemInfo() throws Exception {
        var client = new SystemClientImpl(new SystemService() {
            @Override
            public FutureResponse<SystemResponse.SystemInfo> send(SystemRequest.GetSystemInfo request) throws IOException {
                return FutureResponse.returns(SystemResponse.SystemInfo.newBuilder()
                                                .setName(testName)
                                                .setVersion(testVersion)
                                                .setDate(testDate)
                                                .setUrl(testUrl)
                                                .build());
            }
        });
        var systemInfo = client.getSystemInfo().await();
        assertEquals(testName, systemInfo.getName());
        assertEquals(testVersion, systemInfo.getVersion());
        assertEquals(testDate, systemInfo.getDate());
        assertEquals(testUrl, systemInfo.getUrl());
    }
}
