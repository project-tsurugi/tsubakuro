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
package com.tsurugidb.tsubakuro.common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.impl.MockWire;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

class SessionBuilderTest {

    @Test
    void create() throws Exception {
        try (var wire = new MockWire()) {
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getCredential(), creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withCredential(creds);
            try (var session = builder.create()) {
                // ok.
            }
        }
    }

    @Test
    void createAsync() throws Exception {
        try (var wire = new MockWire()) {
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getCredential(), creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withCredential(creds);
            try (
                    var fSession = builder.createAsync();
                    var session = fSession.get(10, TimeUnit.SECONDS)) {
                // ok.
            }
        }
    }

    @Test
    void createWithClientInformation() throws Exception {
        try (var wire = new MockWire()) {
            String label = "label for the test";
            String applicationName = "applicationName for the test";
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getConnectionLabel(), label);
                    assertSame(clientInformation.getApplicationName(), applicationName);
                    assertSame(clientInformation.getCredential(), creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withLabel(label).withApplicationName(applicationName).withCredential(creds);
            try (var session = builder.create()) {
                // ok.
            }
        }
    }

    @Test
    void createAsyncWithClientInformation() throws Exception {
        try (var wire = new MockWire()) {
            String label = "label for the test";
            String applicationName = "applicationName for the test";
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getConnectionLabel(), label);
                    assertSame(clientInformation.getApplicationName(), applicationName);
                    assertSame(clientInformation.getCredential(), creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withLabel(label).withApplicationName(applicationName).withCredential(creds);
            try (
                    var fSession = builder.createAsync();
                    var session = fSession.get(10, TimeUnit.SECONDS)) {
                // ok.
            }
        }
    }

    @Test
    void createAsyncGetTwice() throws Exception {
        try (var wire = new MockWire()) {
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getCredential(), creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withCredential(creds);
            try (
                    var fSession = builder.createAsync();
                    var session1 = fSession.get(10, TimeUnit.SECONDS);
                    var session2 = fSession.get(10, TimeUnit.SECONDS)) {
                assertEquals(session1, session2);
            }
        }
    }
}
