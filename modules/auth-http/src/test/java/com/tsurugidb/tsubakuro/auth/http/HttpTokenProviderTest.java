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
package com.tsurugidb.tsubakuro.auth.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;

class HttpTokenProviderTest {

    @Test
    void ctor() {
        var provider = new HttpTokenProvider("https://example.com:8080/auth");
        assertEquals(URI.create("https://example.com:8080/auth/"), provider.getEndpoint());
    }

    @Test
    void ctor_nopath() {
        var provider = new HttpTokenProvider("https://example.com:8080");
        assertEquals(URI.create("https://example.com:8080/"), provider.getEndpoint());
    }

    @Test
    void issue() throws Exception {
        var provider = new HttpTokenProvider("http://example.com/auth") {
            @Override
            Response submit(HttpRequest request) {
                assertEquals(URI.create("http://example.com/auth/issue"), request.uri());
                var auth = request.headers().firstValue("Authorization").get();
                var pair = auth.split("\\s+");
                assertEquals(2, pair.length);
                assertEquals("Basic", pair[0]);
                assertEquals(
                        "user:pass",
                        new String(Base64.getDecoder().decode(pair[1]), StandardCharsets.US_ASCII));
                return new Response(200, MessageType.OK, "TKN", null);
            }
        };
        var token = provider.issue("user", "pass");
        assertEquals("TKN", token);
    }

    @Test
    void issue_auth_error() throws Exception {
        var provider = new HttpTokenProvider("http://example.com/auth") {
            @Override
            Response submit(HttpRequest request) {
                return new Response(401, MessageType.UNKNOWN, null, null);
            }
        };
        var e = assertThrows(CoreServiceException.class, () -> provider.issue("user", "pass"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void issue_server_error() throws Exception {
        var provider = new HttpTokenProvider("http://example.com/auth") {
            @Override
            Response submit(HttpRequest request) {
                return new Response(503, MessageType.UNKNOWN, null, null);
            }
        };
        var e = assertThrows(CoreServiceException.class, () -> provider.issue("user", "pass"));
        assertEquals(CoreServiceCode.SYSTEM_ERROR, e.getDiagnosticCode());
    }

    @Test
    void issue_ok_but_empty() throws Exception {
        var provider = new HttpTokenProvider("http://example.com/auth") {
            @Override
            Response submit(HttpRequest request) {
                return new Response(200, MessageType.OK, null, null);
            }
        };
        assertThrows(InvalidResponseException.class, () -> provider.issue("user", "pass"));
    }

    @Test
    void refresh() throws Exception {
        var provider = new HttpTokenProvider("http://example.com") {
            @Override
            Response submit(HttpRequest request) {
                assertEquals(URI.create("http://example.com/refresh"), request.uri());
                var auth = request.headers().firstValue("Authorization").get();
                var pair = auth.split("\\s+");
                assertEquals(2, pair.length);
                assertEquals("Bearer", pair[0]);
                assertEquals("TKN", pair[1]);

                assertEquals(
                        OptionalLong.empty(),
                        request.headers().firstValueAsLong(HttpTokenProvider.KEY_TOKEN_EXPIRATION));
                return new Response(200, MessageType.OK, "TKN", null);
            }
        };
        var token = provider.refresh("TKN", 0, TimeUnit.SECONDS);
        assertEquals("TKN", token);
    }

    @Test
    void refresh_max_expiration() throws Exception {
        var provider = new HttpTokenProvider("http://example.com") {
            @Override
            Response submit(HttpRequest request) {
                assertEquals(URI.create("http://example.com/refresh"), request.uri());
                assertEquals(
                        OptionalLong.of(600),
                        request.headers().firstValueAsLong(HttpTokenProvider.KEY_TOKEN_EXPIRATION));
                return new Response(200, MessageType.OK, "TKN", null);
            }
        };
        var token = provider.refresh("TKN", 10, TimeUnit.MINUTES);
        assertEquals("TKN", token);
    }

    @Test
    void verify() throws Exception {
        var provider = new HttpTokenProvider("http://example.com/") {
            @Override
            Response submit(HttpRequest request) {
                assertEquals(URI.create("http://example.com/verify"), request.uri());
                var auth = request.headers().firstValue("Authorization").get();
                var pair = auth.split("\\s+");
                assertEquals(2, pair.length);
                assertEquals("Bearer", pair[0]);
                assertEquals("TKN", pair[1]);
                return new Response(200, MessageType.OK, "TKN", null);
            }
        };
        provider.verify("TKN");
    }

    private static HttpTokenProvider returns(int status, MessageType type) {
        return new HttpTokenProvider("http://example.com/") {
            @Override
            Response submit(HttpRequest request) {
                return new Response(status, type, null, type.toString());
            }
        };
    }

    @Test
    void error_auth_error() throws Exception {
        var e = assertThrows(CoreServiceException.class, () -> returns(401, MessageType.AUTH_ERROR).verify("TKN"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void error_no_token() throws Exception {
        var e = assertThrows(CoreServiceException.class, () -> returns(401, MessageType.NO_TOKEN).verify("TKN"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void error_token_expired() throws Exception {
        var e = assertThrows(CoreServiceException.class, () -> returns(401, MessageType.TOKEN_EXPIRED).verify("TKN"));
        assertEquals(CoreServiceCode.REFRESH_EXPIRED, e.getDiagnosticCode());
    }

    @Test
    void error_invalid_audience() throws Exception {
        var e = assertThrows(CoreServiceException.class, () -> returns(401, MessageType.INVALID_AUDIENCE).verify("TKN"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void error_invalid_token() throws Exception {
        var e = assertThrows(CoreServiceException.class, () -> returns(401, MessageType.INVALID_TOKEN).verify("TKN"));
        assertEquals(CoreServiceCode.BROKEN_CREDENTIAL, e.getDiagnosticCode());
    }

    @Test
    void error_unknown() throws Exception {
        var e = assertThrows(CoreServiceException.class, () -> returns(401, MessageType.UNKNOWN).verify("TKN"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void error_server() throws Exception {
        var e = assertThrows(CoreServiceException.class, () -> returns(503, MessageType.UNKNOWN).verify("TKN"));
        assertEquals(CoreServiceCode.SYSTEM_ERROR, e.getDiagnosticCode());
    }

    @Test
    void error_unrecognized() throws Exception {
        assertThrows(InvalidResponseException.class, () -> returns(203, MessageType.UNKNOWN).verify("TKN"));
    }
}
