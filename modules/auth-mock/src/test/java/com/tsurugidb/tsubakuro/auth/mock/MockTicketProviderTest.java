package com.tsurugidb.tsubakuro.auth.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.nautilus_technologies.tsubakuro.exception.CoreServiceCode;
import com.nautilus_technologies.tsubakuro.exception.CoreServiceException;
import com.nautilus_technologies.tsubakuro.low.auth.Ticket;
import com.nautilus_technologies.tsubakuro.low.auth.TokenKind;

class MockTicketProviderTest {

    private static final Instant EPOCH = Instant.ofEpochSecond(123_456_789);

    @Test
    void issue_defaults() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH)
                .withUser("a", "p");
        Ticket ticket = provider.issue("a", "p");

        assertEquals("a", ticket.getUserId());
        assertEquals("a", ticket.getUserName());
        assertEquals(
                Optional.of(EPOCH.plus(Duration.ofSeconds(MockTicketProvider.DEFAULT_REFRESH_EXPIRATION_SECOND))),
                ticket.getRefreshExpirationTime());
        assertEquals(
                Optional.of(EPOCH.plus(Duration.ofSeconds(MockTicketProvider.DEFAULT_ACCESS_EXPIRATION_SECOND))),
                ticket.getAccessExpirationTime());
    }

    @Test
    void issue_user_missing() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH);

        var e = assertThrows(CoreServiceException.class, () -> provider.issue("a", "p"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void issue_password_mismatch() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH)
                .withUser("a", "OTHER");

        var e = assertThrows(CoreServiceException.class, () -> provider.issue("a", "p"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void issue_disable_refresh() throws Exception {
        var provider = new MockTicketProvider()
                .withRefreshExpiration(-1, TimeUnit.SECONDS)
                .withUser("a", "p");
        Ticket ticket = provider.issue("a", "p");
        assertEquals(Optional.empty(), ticket.getRefreshExpirationTime());
        assertNotEquals(Optional.empty(), ticket.getAccessExpirationTime());
    }

    @Test
    void issue_disable_access() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH)
                .withAccessExpiration(-1, TimeUnit.SECONDS)
                .withUser("a", "p");
        Ticket ticket = provider.issue("a", "p");
        assertNotEquals(Optional.empty(), ticket.getRefreshExpirationTime());
        assertEquals(Optional.empty(), ticket.getAccessExpirationTime());
    }

    @Test
    void refresh() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(Instant.now())
                .withRefreshExpiration(1, TimeUnit.MINUTES)
                .withAccessExpiration(-1, TimeUnit.SECONDS)
                .withUser("a", "p");
        Ticket ticket = provider.issue("a", "p");

        provider.withIssuedAt(EPOCH);
        Ticket refreshed = provider.refresh(ticket, 1, TimeUnit.HOURS);
        assertEquals(ticket.getToken(TokenKind.REFRESH), refreshed.getToken(TokenKind.REFRESH));

        assertNotSame(ticket, refreshed);
        assertEquals(Optional.of(EPOCH.plus(Duration.ofHours(1))), refreshed.getAccessExpirationTime());
    }

    @Test
    void refresh_expired() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(Instant.now().minus(Duration.ofSeconds(3))) // issues at the past
                .withAccessExpiration(-1, TimeUnit.SECONDS)
                .withRefreshExpiration(0, TimeUnit.SECONDS)
                .withUser("a", "p");
        Ticket ticket = provider.issue("a", "p");
        var e = assertThrows(CoreServiceException.class, () -> provider.refresh(ticket, 1, TimeUnit.HOURS));
        assertEquals(CoreServiceCode.REFRESH_EXPIRED, e.getDiagnosticCode());
    }

    @Test
    void refresh_unsupported() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH)
                .withRefreshExpiration(-1, TimeUnit.SECONDS)
                .withUser("a", "p");
        Ticket ticket = provider.issue("a", "p");
        assertThrows(IllegalArgumentException.class, () -> provider.refresh(ticket, 1,TimeUnit.HOURS));
    }

    @Test
    void restore() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH)
                .withUser("a", "p");
        Ticket ticket = provider.issue("a", "p");

        Ticket rt = provider.restore(ticket.getToken(TokenKind.REFRESH).get());
        assertEquals(ticket.getUserId(), rt.getUserId());
        assertEquals(ticket.getUserName(), rt.getUserName());
        assertEquals(ticket.getRefreshExpirationTime(), rt.getRefreshExpirationTime());
        assertEquals(Optional.empty(), rt.getAccessExpirationTime());
        assertEquals(ticket.getToken(TokenKind.REFRESH), rt.getToken(TokenKind.REFRESH));

        Ticket at = provider.restore(ticket.getToken(TokenKind.ACCESS).get());
        assertEquals(ticket.getUserId(), at.getUserId());
        assertEquals(ticket.getUserName(), at.getUserName());
        assertEquals(Optional.empty(), at.getRefreshExpirationTime());
        assertEquals(ticket.getAccessExpirationTime(), at.getAccessExpirationTime());
        assertEquals(ticket.getToken(TokenKind.ACCESS), at.getToken(TokenKind.ACCESS));
    }

    @Test
    void restore_issuer_invalid() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH);
        String token = JWT.create()
                .withIssuer("INVALID")
                .withSubject(MockTicketProvider.CLAIM_SUBJECT_ACCESS)
                .withAudience("testing")
                .withIssuedAt(Date.from(Instant.now()))
                .withExpiresAt(Date.from(Instant.now()))
                .sign(Algorithm.none());
        assertThrows(IllegalArgumentException.class, () -> provider.restore(token));
    }

    @Test
    void restore_subject_invalid() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH);
        String token = JWT.create()
                .withIssuer(MockTicketProvider.CLAIM_ISSUER)
                .withSubject("INVALID")
                .withAudience("testing")
                .withIssuedAt(Date.from(Instant.now()))
                .withExpiresAt(Date.from(Instant.now()))
                .sign(Algorithm.none());
        assertThrows(IllegalArgumentException.class, () -> provider.restore(token));
    }

    @Test
    void restore_audience_missing() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH);
        String token = JWT.create()
                .withIssuer(MockTicketProvider.CLAIM_ISSUER)
                .withSubject(MockTicketProvider.CLAIM_SUBJECT_ACCESS)
                .withAudience()
                .withIssuedAt(Date.from(Instant.now()))
                .withExpiresAt(Date.from(Instant.now()))
                .sign(Algorithm.none());
        assertThrows(IllegalArgumentException.class, () -> provider.restore(token));
    }

    @Test
    void restore_audience_too_much() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH);
        String token = JWT.create()
                .withIssuer(MockTicketProvider.CLAIM_ISSUER)
                .withSubject(MockTicketProvider.CLAIM_SUBJECT_ACCESS)
                .withAudience("a", "b", "c")
                .withIssuedAt(Date.from(Instant.now()))
                .withExpiresAt(Date.from(Instant.now()))
                .sign(Algorithm.none());
        assertThrows(IllegalArgumentException.class, () -> provider.restore(token));
    }

    @Test
    void restore_issued_at_missing() throws Exception {
        var provider = new MockTicketProvider()
                .withIssuedAt(EPOCH);
        String token = JWT.create()
                .withIssuer(MockTicketProvider.CLAIM_ISSUER)
                .withSubject(MockTicketProvider.CLAIM_SUBJECT_ACCESS)
                .withAudience("user")
                .withIssuedAt(Date.from(Instant.now()))
                .sign(Algorithm.none());
        assertThrows(IllegalArgumentException.class, () -> provider.restore(token));
    }

    @Test
    void restore_expires_at_missing() throws Exception {
        var provider = new MockTicketProvider();
        String token = JWT.create()
                .withIssuer(MockTicketProvider.CLAIM_ISSUER)
                .withSubject(MockTicketProvider.CLAIM_SUBJECT_ACCESS)
                .withAudience("user")
                .withExpiresAt(Date.from(Instant.now()))
                .sign(Algorithm.none());
        assertThrows(IllegalArgumentException.class, () -> provider.restore(token));
    }
}
