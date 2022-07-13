package com.nautilus_technologies.tsubakuro.impl.low.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.nautilus_technologies.tsubakuro.low.auth.TokenKind;

class JwtTicketTest {

    @Test
    void fromTokenAccess() {
        var token = JWT.create()
                .withExpiresAt(Date.from(Instant.ofEpochSecond(100)))
                .sign(Algorithm.none());
        var ticket = JwtTicket.fromToken(null, token, t -> TokenKind.ACCESS , t -> "uid", null);

        assertEquals("uid", ticket.getUserId());
        assertEquals("uid", ticket.getUserName());

        assertEquals(Optional.of(Instant.ofEpochSecond(100)), ticket.getAccessExpirationTime());
        assertEquals(Optional.empty(), ticket.getRefreshExpirationTime());

        assertEquals(Optional.of(token), ticket.getToken(TokenKind.ACCESS));
        assertEquals(Optional.empty(), ticket.getToken(TokenKind.REFRESH));
        assertEquals(token, ticket.getToken());
    }

    @Test
    void fromTokenRefresh() {
        var token = JWT.create()
                .withExpiresAt(Date.from(Instant.ofEpochSecond(100)))
                .sign(Algorithm.none());
        var ticket = JwtTicket.fromToken(null, token, t -> TokenKind.REFRESH, t -> "uid", t -> "un");

        assertEquals("uid", ticket.getUserId());
        assertEquals("un", ticket.getUserName());

        assertEquals(Optional.empty(), ticket.getAccessExpirationTime());
        assertEquals(Optional.of(Instant.ofEpochSecond(100)), ticket.getRefreshExpirationTime());

        assertEquals(Optional.empty(), ticket.getToken(TokenKind.ACCESS));
        assertEquals(Optional.of(token), ticket.getToken(TokenKind.REFRESH));
        assertEquals(token, ticket.getToken());
    }

    @Test
    void merge() {
        var at = JWT.create()
                .withExpiresAt(Date.from(Instant.ofEpochSecond(100)))
                .sign(Algorithm.none());
        var ticket = JwtTicket.fromToken(null, at, t -> TokenKind.ACCESS, t -> "a", t -> "an");

        var rt = JWT.create()
                .withExpiresAt(Date.from(Instant.ofEpochSecond(200)))
                .sign(Algorithm.none());
        ticket = ticket.merge(
                TokenKind.REFRESH,
                JwtTicket.fromToken(null, rt, t -> TokenKind.REFRESH, t -> "r", t -> "rn"));


        assertEquals("a", ticket.getUserId());
        assertEquals("an", ticket.getUserName());

        assertEquals(Optional.of(Instant.ofEpochSecond(100)), ticket.getAccessExpirationTime());
        assertEquals(Optional.of(Instant.ofEpochSecond(200)), ticket.getRefreshExpirationTime());

        assertEquals(Optional.of(at), ticket.getToken(TokenKind.ACCESS));
        assertEquals(Optional.of(rt), ticket.getToken(TokenKind.REFRESH));
        assertEquals(rt, ticket.getToken());
    }
}
