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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.auth.proto.AuthRequest;
import com.tsurugidb.tsubakuro.auth.AuthInfo;
import com.tsurugidb.tsubakuro.auth.AuthService;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class AuthClientImplTest {

    @Test
    void getAuthInfo() throws Exception {
        var client = new AuthClientImpl(new AuthService() {
            @Override
            public FutureResponse<AuthInfo> send(AuthRequest.AuthInfo request) throws IOException {
                return FutureResponse.returns(new AuthInfo("A", new RememberMeCredential("T")));
            }
        });
        var info = client.getAuthInfo().await();
        assertEquals("A", info.getName());
        assertEquals("T", info.getToken().getToken());
    }
}
