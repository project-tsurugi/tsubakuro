package com.tsurugidb.tsubakuro.auth.http;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.jwt.JWT;
import com.tsurugidb.tsubakuro.auth.Ticket;
import com.tsurugidb.tsubakuro.auth.TicketProvider;
import com.tsurugidb.tsubakuro.auth.TokenKind;
import com.tsurugidb.tsubakuro.auth.impl.JwtTicket;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;

/**
 * An implementation of {@link TicketProvider} that provides JWT based ticket.
 * @see <a href="https://github.com/project-tsurugi/harinoki/blob/master/docs/token-ja.md">authentication token specification</a>
 */
public class JwtTicketProvider implements TicketProvider {

    /**
     * The JWT claim name of authenticated user name.
     */
    public static final String CLAIM_USER_NAME = "tsurugi/auth/name"; //$NON-NLS-1$

    /**
     * The JWT subject value for access tokens.
     */
    public static final String SUBJECT_ACCESS_TOKEN = "access";

    /**
     * The JWT subject value for access tokens.
     */
    public static final String SUBJECT_REFRESH_TOKEN = "refresh";

    static final Logger LOG = LoggerFactory.getLogger(JwtTicketProvider.class);

    private final TokenProvider tokenProvider;

    /**
     * Creates a new instance.
     * @param tokenProvider the token provider
     */
    public JwtTicketProvider(@Nonnull TokenProvider tokenProvider) {
        Objects.requireNonNull(tokenProvider);
        this.tokenProvider = tokenProvider;
    }

    @Override
    public Ticket restore(@Nonnull String text) {
        Objects.requireNonNull(text);
        return restore(text, IllegalArgumentException::new);
    }

    @Override
    public Ticket issue(@Nonnull String userId, @Nonnull String password)
            throws InterruptedException, IOException, CoreServiceException {
        Objects.requireNonNull(userId);
        Objects.requireNonNull(password);
        var token = tokenProvider.issue(userId, password);
        var ticket = restore(token, IllegalStateException::new);
        return ticket;
    }

    @Override
    public Ticket refresh(@Nonnull Ticket ticket, long expiration, @Nonnull TimeUnit unit)
            throws InterruptedException, IOException, CoreServiceException {
        Objects.requireNonNull(ticket);
        Objects.requireNonNull(unit);
        var jwt = validate(ticket);

        var token = ticket.getToken(TokenKind.REFRESH);
        if (ticket.getRefreshExpirationTime().isEmpty() || token.isEmpty()) {
            throw new IllegalArgumentException("the ticket does not support refresh");
        }
        var accessToken = tokenProvider.refresh(token.get(), expiration, unit);
        var accessTicket = restore(accessToken, IllegalStateException::new);
        if (accessTicket.getAccessExpirationTime().isEmpty()) {
            throw new IllegalStateException("retrieved access token was broken");
        }
        return jwt.merge(TokenKind.ACCESS, accessTicket);
    }

    /**
     * Verifies a token in the given ticket.
     * Note that, this never considers whether the token was expired.
     * @param ticket the ticket
     * @param kind the token kind to verify
     * @throws IllegalArgumentException if such the token is absent in the given ticket
     * @throws InterruptedException if interrupted while requesting authentication to the service
     * @throws IOException if I/O error was occurred while requesting authentication to the service
     * @throws CoreServiceException if the ticket is not valid or
     */
    public void verify(@Nonnull Ticket ticket, @Nonnull TokenKind kind)
            throws InterruptedException, IOException, CoreServiceException {
        Objects.requireNonNull(ticket);
        Objects.requireNonNull(kind);
        validate(ticket);
        var token = ticket.getToken(kind)
                .orElseThrow(() -> new IllegalArgumentException(MessageFormat.format(
                        "auth token is absent: kind={0}",
                        kind)));
        tokenProvider.verify(token);
    }

    private <T extends Throwable> JwtTicket restore(String text, Function<String, T> exceptionFactory) throws T {
        LOG.trace("decoding token: {}", text);
        var token = JWT.decode(text);
        LOG.trace("token was decoded: {}", token.getClaims());
        TokenKind kind = null;
        if (token.getIssuer() == null) {
            throw exceptionFactory.apply("token issuer must be set");
        }
        if (Objects.equals(token.getSubject(), SUBJECT_ACCESS_TOKEN)) {
            kind = TokenKind.ACCESS;
        } else if (Objects.equals(token.getSubject(), SUBJECT_REFRESH_TOKEN)) {
            kind = TokenKind.REFRESH;
        } else {
            throw exceptionFactory.apply(MessageFormat.format(
                    "invalid token subject: \"{0}\" (expect: \"{1}\" or \"{2}\")",
                    token.getSubject(),
                    SUBJECT_ACCESS_TOKEN,
                    SUBJECT_REFRESH_TOKEN));
        }
        if (token.getAudience() == null || token.getAudience().isEmpty()) {
            throw exceptionFactory.apply("token audience must be set");
        }
        if (token.getIssuedAt() == null) {
            throw exceptionFactory.apply("token issued_at must be set");
        }
        if (token.getExpiresAt() == null) {
            throw exceptionFactory.apply("token expires_at must be set");
        }
        var name = token.getClaim(CLAIM_USER_NAME).asString();
        if (name == null) {
            throw exceptionFactory.apply("authenticated user name must be set");
        }
        LOG.trace("building a JWT ticket: name={}, kind={}", name, kind);
        return new JwtTicket(this, name, name, Map.of(kind, token));
    }

    private JwtTicket validate(Ticket ticket) {
        if (ticket instanceof JwtTicket) {
            var jwt = (JwtTicket) ticket;
            if (Objects.equals(jwt.getProvider(), this)) {
                return jwt;
            }
        }
        throw new IllegalArgumentException("ticket is imcompatible for the provider");
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tokenProvider);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        JwtTicketProvider other = (JwtTicketProvider) obj;
        return Objects.equals(tokenProvider, other.tokenProvider);
    }

    @Override
    public String toString() {
        return String.format("JwtTicketProvider(%s)", tokenProvider); //$NON-NLS-1$
    }
}
