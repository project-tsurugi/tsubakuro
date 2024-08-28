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

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.tsurugidb.tsubakuro.auth.TokenKind;

class JwtTicketProviderTest {

    static class MockTokenProvider implements TokenProvider {

        String issuer = "i";

        String audience = "a";

        String userName = "u";

        Instant issuedAt = Instant.ofEpochSecond(10);

        Instant expiresAt = Instant.ofEpochSecond(20);

        Algorithm algorithm = Algorithm.HMAC256("testing");

        String issueKind = JwtTicketProvider.SUBJECT_REFRESH_TOKEN;

        String refreshKind = JwtTicketProvider.SUBJECT_ACCESS_TOKEN;

        @Override
        public String issue(String user, String password) {
            return JWT.create()
                    .withIssuer(issuer)
                    .withSubject(issueKind)
                    .withAudience(audience)
                    .withIssuedAt(Date.from(issuedAt))
                    .withExpiresAt(Date.from(expiresAt))
                    .withClaim(JwtTicketProvider.CLAIM_USER_NAME, userName)
                    .sign(algorithm);
        }

        @Override
        public String refresh(String token, long expiration, TimeUnit unit) {
            return JWT.create()
                    .withIssuer(issuer)
                    .withSubject(refreshKind)
                    .withAudience(audience)
                    .withIssuedAt(Date.from(issuedAt))
                    .withExpiresAt(Date.from(expiresAt))
                    .withClaim(JwtTicketProvider.CLAIM_USER_NAME, userName)
                    .sign(algorithm);
        }

        @Override
        public void verify(String token) {
            var jwt = JWT.decode(token);
            try {
                JWT.require(algorithm).build().verify(jwt);
            } catch (TokenExpiredException e) {
                e.toString();
            }
        }
    }

    @Test
    void issue() throws Exception {
        var tokens = new MockTokenProvider();
        var provider = new JwtTicketProvider(tokens);
        var ticket = provider.issue("user", "pass");
        assertEquals("u", ticket.getUserId());
        assertEquals("u", ticket.getUserName());
        assertEquals(Optional.empty(), ticket.getAccessExpirationTime());
        assertEquals(Optional.of(Instant.ofEpochSecond(20)), ticket.getRefreshExpirationTime());
    }

    @Test
    void refresh() throws Exception {
        var tokens = new MockTokenProvider();
        var provider = new JwtTicketProvider(tokens);

        var ticket = provider.issue("user", "pass");
        assertEquals(Optional.empty(), ticket.getAccessExpirationTime());
        assertEquals(Optional.of(Instant.ofEpochSecond(20)), ticket.getRefreshExpirationTime());

        tokens.expiresAt = Instant.ofEpochSecond(30);
        var refreshed = provider.refresh(ticket);
        assertEquals(Optional.of(Instant.ofEpochSecond(30)), refreshed.getAccessExpirationTime());
        assertEquals(Optional.of(Instant.ofEpochSecond(20)), refreshed.getRefreshExpirationTime());
    }

    @Test
    void refresh_access() throws Exception {
        var tokens = new MockTokenProvider();
        var provider = new JwtTicketProvider(tokens);

        tokens.issueKind = JwtTicketProvider.SUBJECT_ACCESS_TOKEN;
        var ticket = provider.issue("user", "pass");
        assertThrows(IllegalArgumentException.class, () -> provider.refresh(ticket));
    }

    @Test
    void refresh_returns_refresh() throws Exception {
        var tokens = new MockTokenProvider();
        var provider = new JwtTicketProvider(tokens);

        var ticket = provider.issue("user", "pass");
        tokens.refreshKind = JwtTicketProvider.SUBJECT_REFRESH_TOKEN;
        assertThrows(IllegalStateException.class, () -> provider.refresh(ticket));
    }

    @Test
    void verify() throws Exception {
        var tokens = new MockTokenProvider();
        var provider = new JwtTicketProvider(tokens);

        tokens.issueKind = JwtTicketProvider.SUBJECT_ACCESS_TOKEN;
        var ticket = provider.issue("user", "pass");
        provider.verify(ticket, TokenKind.ACCESS);
    }

    @Test
    void verify_absent() throws Exception {
        var tokens = new MockTokenProvider();
        var provider = new JwtTicketProvider(tokens);

        var ticket = provider.issue("user", "pass");
        assertThrows(IllegalArgumentException.class, () -> provider.verify(ticket, TokenKind.ACCESS));
    }

    @Test
    void restore() throws Exception {
        var tokens = new MockTokenProvider();
        var provider = new JwtTicketProvider(tokens);

        var ticket = provider.issue("user", "pass");
        var token = ticket.getToken(TokenKind.REFRESH).get();

        var restored = provider.restore(token);
        assertEquals(Optional.empty(), restored.getAccessExpirationTime());
        assertEquals(Optional.of(Instant.ofEpochSecond(20)), restored.getRefreshExpirationTime());

        provider.verify(ticket, TokenKind.REFRESH);
    }

    @Test
    void malformed_subject() throws Exception {
        var tokens = new MockTokenProvider();

        tokens.issueKind = "X";
        var provider = new JwtTicketProvider(tokens);
        assertThrows(IllegalStateException.class, () -> provider.issue("user", "pass"));
    }

    @Test
    void mismatch_issuer() throws Exception {
        var a = new JwtTicketProvider(new MockTokenProvider());
        var b = new JwtTicketProvider(new MockTokenProvider());
        var ticket = a.issue("user", "pass");
        assertThrows(IllegalArgumentException.class, () -> b.refresh(ticket));
    }
}
