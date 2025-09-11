/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.StringWriter;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Message;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonToken;

import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointRequest.Credential;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.diagnostics.proto.Diagnostics;
import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.mock.ResponseProtoForTests;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class HandshakeTest {
    private final MockLink link = new MockLink();
    private WireImpl wire = null;

    public HandshakeTest() {
        try {
            wire = new WireImpl(link);
        } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }
    }

    @Test
    void handshake_without_name() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                                    .setSessionId(123))
                    .build());

        // send request via product functionality
        var future = wire.handshake(new ClientInformation(), null, 0, null);
        assertNotNull(future);
        long sessionId = future.get();

        // check the situation when the response is received
        assertEquals(sessionId, 123);
        assertEquals(wire.getUserName().get(), Optional.empty());
    }

    @Test
    void handshake_with_name() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder()
                                    .setSessionId(123)
                                    .setUserName("TestUser"))
                    .build());

        // send request via product functionality
        var future = wire.handshake(new ClientInformation(), null, 0, null);
        assertNotNull(future);
        long sessionId = future.get();

        // check the situation when the response is received
        assertEquals(sessionId, 123);
        assertEquals(wire.getUserName().get().get(), "TestUser");
    }

    @Test
    void handshake_authentication_error() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.Handshake.newBuilder()
                    .setError(EndpointResponse.Error.newBuilder()
                                    .setCode(Diagnostics.Code.AUTHENTICATION_ERROR)
                                    .setMessage("Authentication failed"))
                    .build());

        var future = wire.handshake(new ClientInformation(), null, 0, null);
        assertNotNull(future);
        CoreServiceException e = assertThrows(CoreServiceException.class, () -> future.get());
        assertEquals(e.getDiagnosticCode(), CoreServiceCode.AUTHENTICATION_ERROR);
        CoreServiceException eun = assertThrows(CoreServiceException.class, () -> wire.getUserName().get());
        assertEquals(eun.getDiagnosticCode(), CoreServiceCode.AUTHENTICATION_ERROR);
    }

    // userPassword cases
    @Test
    void handshake_authentication_userPassword_success() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.EncryptionKey.newBuilder()
                    .setSuccess(EndpointResponse.EncryptionKey.Success.newBuilder()
                                   .setEncryptionKey(ResponseProtoForTests.encryptionKey()))
                    .build());

        link.next(EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder())
                    .build());

        var clientInformation = new ClientInformation(null, null, new UsernamePasswordCredential("user", "password"));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);
        future.get();

        assertFalse(link.hasRemaining());
    }

    @Test
    void handshake_authentication_userPassword_UNSUPPORTED_OPERATION() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.EncryptionKey.newBuilder()
                    .setError(EndpointResponse.Error.newBuilder()
                                   .setCode(Diagnostics.Code.UNSUPPORTED_OPERATION))
                    .build());

        link.next(EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder())
                    .build());

        var clientInformation = new ClientInformation(null, null, new UsernamePasswordCredential("user", "password"));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);
        future.get();

        assertFalse(link.hasRemaining());
    }

    @Test
    void handshake_authentication_userPassword_RESOURCE_LIMIT_REACHED() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.EncryptionKey.newBuilder()
                    .setError(EndpointResponse.Error.newBuilder()
                                   .setCode(Diagnostics.Code.RESOURCE_LIMIT_REACHED))
                    .build());

        var clientInformation = new ClientInformation(null, null, new UsernamePasswordCredential("user", "password"));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);
        assertThrows(ConnectException.class, () -> future.get());

        assertFalse(link.hasRemaining());
    }

    @Test
    void handshake_authentication_userPassword_ILLEGAL_ARGUMENT() throws Exception {
        // push response message via test functionality
        link.next(FrameworkResponse.Header.newBuilder().setPayloadType(FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS).build(),
                  Diagnostics.Record.newBuilder().setCode(Diagnostics.Code.INVALID_REQUEST).build());

        var clientInformation = new ClientInformation(null, null, new UsernamePasswordCredential("user", "password"));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);
        assertThrows(CoreServiceException.class, () -> future.get());

        assertFalse(link.hasRemaining());
    }

    @Test
    // cf. java.lang.IllegalArgumentException: javax.crypto.IllegalBlockSizeException: Data must not be longer than 245 bytes
    void handshake_authentication_userPassword_long_string_OK() throws Exception {
        String user = "u".repeat(60);
        String password = "p".repeat(60);

        // push response message via test functionality
        link.next(EndpointResponse.EncryptionKey.newBuilder()
                    .setSuccess(EndpointResponse.EncryptionKey.Success.newBuilder()
                                   .setEncryptionKey(ResponseProtoForTests.encryptionKey()))
                    .build());

        link.next(EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder())
                    .build());

        var clientInformation = new ClientInformation(null, null, new UsernamePasswordCredential(user, password));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);
        future.get();

        assertFalse(link.hasRemaining());
    }

    @Test
    void handshake_authentication_userPassword_long_name_NG() throws Exception {
        String user = "u".repeat(61);
        String password = "p".repeat(60);

        var ex = assertThrows(IllegalArgumentException.class, () -> new ClientInformation(null, null, new UsernamePasswordCredential(user, password)));
        System.out.println(ex);
        assertFalse(link.hasRemaining());
    }

    @Test
    void handshake_authentication_userPassword_long_password_NG() throws Exception {
        String user = "u".repeat(60);
        String password = "p".repeat(61);

        var ex = assertThrows(IllegalArgumentException.class, () -> new ClientInformation(null, null, new UsernamePasswordCredential(user, password)));
        System.out.println(ex);
        assertFalse(link.hasRemaining());
    }

    // rememberMe cases
    @Test
    void handshake_authentication_rememberMe_success() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder())
                    .build());

        var clientInformation = new ClientInformation(null, null, new RememberMeCredential("token"));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);
        future.get();

        assertFalse(link.hasRemaining());
    }

    @Test
    void handshake_authentication_rememberMe_RESOURCE_LIMIT_REACHED() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.Handshake.newBuilder()
                    .setError(EndpointResponse.Error.newBuilder()
                                .setCode(Diagnostics.Code.RESOURCE_LIMIT_REACHED))
                    .build());

        var clientInformation = new ClientInformation(null, null, new RememberMeCredential("token"));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);
        assertThrows(ConnectException.class, () -> future.get());

        assertFalse(link.hasRemaining());
    }

    @Test
    void handshake_authentication_rememberMe_ILLEGAL_ARGUMENT() throws Exception {
        // push response message via test functionality
        link.next(FrameworkResponse.Header.newBuilder().setPayloadType(FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS).build(),
                  Diagnostics.Record.newBuilder().setCode(Diagnostics.Code.INVALID_REQUEST).build());

        var clientInformation = new ClientInformation(null, null, new RememberMeCredential("token"));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertThrows(CoreServiceException.class, () -> future.get());

        assertFalse(link.hasRemaining());
    }

    @Test
    void encrypt_decrypt() throws Exception {
        // push response message via test functionality
        link.next(EndpointResponse.EncryptionKey.newBuilder()
                    .setSuccess(EndpointResponse.EncryptionKey.Success.newBuilder()
                                   .setEncryptionKey(ResponseProtoForTests.encryptionKey()))
                    .build());

        link.next(EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder())
                    .build());

        String user = " Hello, World!".repeat(3);
        String password = " Goodbye, Space".repeat(3);

        var clientInformation = new ClientInformation(null, null, new UsernamePasswordCredential(user, password));
        var future = wire.handshake(clientInformation, null, 0, null);
        assertNotNull(future);

        var decrypter = new Decrypto(ResponseProtoForTests.privateKey());
        var payload = link.getJustBeforePayload();
        try (var in = new ByteBufferInputStream(ByteBuffer.wrap(payload))) {
            var request = EndpointRequest.Request.parseDelimitedFrom(in);
            assertSame(request.getCommandCase(), EndpointRequest.Request.CommandCase.HANDSHAKE);
            var credential = request.getHandshake().getClientInformation().getCredential();
            assertSame(credential.getCredentialOptCase(), EndpointRequest.Credential.CredentialOptCase.ENCRYPTED_CREDENTIAL);
            String jsonText = decrypter.decryptByPrivateKey(credential.getEncryptedCredential());

            JsonFactory JSON = new JsonFactoryBuilder().build();
            var parser = JSON.createParser(jsonText);
            JsonToken token;
            while ((token = parser.nextToken()) != null) {
                if (token == JsonToken.FIELD_NAME) {
                    String name = parser.getCurrentName();
                    token = parser.nextToken();
                    if ("user".equals(name)) {
                        assertEquals(parser.getText(), user);
                    } else if ("password".equals(name)) {
                        assertEquals(parser.getText(), password);
                    }
                }
            }
            parser.close();
        }

        future.get();
        assertFalse(link.hasRemaining());
    }
}