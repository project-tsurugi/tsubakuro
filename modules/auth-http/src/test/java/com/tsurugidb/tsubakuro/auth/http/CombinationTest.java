package com.tsurugidb.tsubakuro.auth.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.auth.TokenKind;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;

/**
 * Combination test between Harinoki server/client.
 */
class CombinationTest {

    static final Logger LOG = LoggerFactory.getLogger(CombinationTest.class);

    static final String KEY_ENDPOINT = "harinoki.endpoint";

    static final String KEY_LOGIN = "harinoki.login";

    private static String endpoint;

    private static String user;

    private static String password;

    @BeforeAll
    static void setup() {
        endpoint = Optional.ofNullable(System.getProperty(KEY_ENDPOINT))
                .filter(it -> !it.isBlank())
                .orElse(null);
        Assumptions.assumeTrue(endpoint != null, String.format("%s is not defined", KEY_ENDPOINT));

        var login = Optional.ofNullable(System.getProperty(KEY_LOGIN))
                .filter(it -> !it.isBlank())
                .orElse(null);
        Assumptions.assumeTrue(login != null, String.format("%s is not defined", KEY_LOGIN));
        var loginPair = login.split(":", 2);
        assertEquals(2, loginPair.length, String.format("%s must be username:password", KEY_LOGIN));
        user = loginPair[0];
        password = loginPair[1];

        LOG.info("harinoki combination test endpoint: {}", endpoint);
        LOG.info("harinoki combination test login user: {}", user);
    }

    @Test
    void issue() throws Exception {
        var now = Instant.now().minus(Duration.ofSeconds(1));

        var provider = new JwtTicketProvider(new HttpTokenProvider(endpoint));
        var ticket = provider.issue(user, password);
        assertEquals(user, ticket.getUserId());
        assertEquals(Optional.empty(), ticket.getAccessExpirationTime());
        assertNotEquals(Optional.empty(), ticket.getRefreshExpirationTime());

        var expiration = ticket.getRefreshExpirationTime().get();
        assertTrue(expiration.isAfter(now));
    }

    @Test
    void issue_invalid_credential() throws Exception {
        var provider = new JwtTicketProvider(new HttpTokenProvider(endpoint));
        var e = assertThrows(CoreServiceException.class, () -> provider.issue(user, password + "???"));
        assertEquals(CoreServiceCode.AUTHENTICATION_ERROR, e.getDiagnosticCode());
    }

    @Test
    void issue_invalid_endpoint() throws Exception {
        var provider = new JwtTicketProvider(new HttpTokenProvider(endpoint + "/INVALID+SUFFIX"));
        assertThrows(InvalidResponseException.class, () -> provider.issue(user, password));
    }

    @Test
    void refresh() throws Exception {
        var now = Instant.now().minus(Duration.ofSeconds(1));

        var provider = new JwtTicketProvider(new HttpTokenProvider(endpoint));
        var ticket = provider.issue(user, password);
        ticket = provider.refresh(ticket);
        assertEquals(user, ticket.getUserId());
        assertNotEquals(Optional.empty(), ticket.getAccessExpirationTime());
        assertNotEquals(Optional.empty(), ticket.getRefreshExpirationTime());


        var expiration = ticket.getAccessExpirationTime().get();
        assertTrue(expiration.isAfter(now));
    }

    @Test
    void refresh_max_expiration() throws Exception {
        var provider = new JwtTicketProvider(new HttpTokenProvider(endpoint));
        var ticket = provider.issue(user, password);
        ticket = provider.refresh(ticket, 10, TimeUnit.SECONDS);
        assertEquals(user, ticket.getUserId());
        assertNotEquals(Optional.empty(), ticket.getAccessExpirationTime());
        assertNotEquals(Optional.empty(), ticket.getRefreshExpirationTime());

        var mayExpire = Instant.now().plus(Duration.ofSeconds(30));
        var expiration = ticket.getAccessExpirationTime().get();
        assertTrue(expiration.isBefore(mayExpire));
    }

    @Test
    void verify() throws Exception {
        var provider = new JwtTicketProvider(new HttpTokenProvider(endpoint));
        var ticket = provider.issue(user, password);
        provider.verify(ticket, TokenKind.REFRESH);
    }

    @Test
    void verify_access() throws Exception {
        var provider = new JwtTicketProvider(new HttpTokenProvider(endpoint));
        var ticket = provider.issue(user, password);
        ticket = provider.refresh(ticket);
        provider.verify(ticket, TokenKind.ACCESS);
    }
}
