package com.tsurugidb.tsubakuro.auth.mock;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.auth.impl.JwtTicket;
import com.tsurugidb.tsubakuro.auth.Ticket;
import com.tsurugidb.tsubakuro.auth.TicketProvider;
import com.tsurugidb.tsubakuro.auth.TokenKind;

/**
 * Mock implementation of {@link TicketProvider}.
 * <p>
 * This ticket is not compatible with Tsurugi OLTP services.
 * </p>
 */
public class MockTicketProvider implements TicketProvider {

    /**
     * The issuer name.
     */
    public static final String CLAIM_ISSUER = "mock"; //$NON-NLS-1$

    /**
     * The subject for access tokens.
     */
    public static final String CLAIM_SUBJECT_ACCESS = "access"; //$NON-NLS-1$

    /**
     * The subject for refresh tokens.
     */
    public static final String CLAIM_SUBJECT_REFRESH = "refresh"; //$NON-NLS-1$

    /**
     * The default value of refresh expiration time in seconds (default: 6 hours).
     */
    public static final long DEFAULT_REFRESH_EXPIRATION_SECOND = TimeUnit.HOURS.toSeconds(6);

    /**
     * The default value of access expiration time in seconds (default: 10 minutes).
     */
    public static final long DEFAULT_ACCESS_EXPIRATION_SECOND = TimeUnit.MINUTES.toSeconds(10);

    private static final Map<TokenKind, String> CLAIM_SUBJECT = new EnumMap<>(Map.of(
            TokenKind.REFRESH, CLAIM_SUBJECT_REFRESH,
            TokenKind.ACCESS, CLAIM_SUBJECT_ACCESS));

    private final Map<String, String> credentials = new HashMap<>();

    private Instant issued = null;

    private long refreshExpirationSec = DEFAULT_REFRESH_EXPIRATION_SECOND;

    private long accessExpirationSec = DEFAULT_ACCESS_EXPIRATION_SECOND;

    /**
     * Adds a pair of user name and its password.
     * <p>
     * If such the user name is already added, this replaces the password into the new one.
     * </p>
     * @param user the user name
     * @param password the password
     * @return this
     * @see #issue(String, String)
     */
    public MockTicketProvider withUser(@Nonnull String user, @Nonnull String password) {
        Objects.requireNonNull(user);
        Objects.requireNonNull(password);
        credentials.put(user, password);
        return this;
    }

    /**
     * Sets the issued date of individual tokens.
     * This is designed only for testing.
     * @param date the issued date, or {@code null} to use {@link Instant#now()}
     * @return this
     */
    public MockTicketProvider withIssuedAt(@Nullable Instant date) {
        issued = date;
        return this;
    }

    /**
     * Sets the expiration time for refresh tokens to be issued.
     * If {@code time} is set to a negative number, {@link #issue(String, String) issue()} will not provide a refresh token.
     * @param time the expiration time from issued date
     * @param unit the time unit
     * @return this
     * @see #withIssuedAt(Instant)
     */
    public MockTicketProvider withRefreshExpiration(long time, @Nonnull TimeUnit unit) {
        Objects.requireNonNull(unit);
        refreshExpirationSec = toSec(time, unit);
        return this;
    }

    /**
     * Sets the expiration time for access tokens to be issued.
     * If {@code time} is set to a negative number, {@link #issue(String, String) issue()} will not provide an access token.
     * Note that, {@link #refresh(Ticket, long, TimeUnit) refresh()} will ignore this setting.
     * @param time the expiration time from issued date
     * @param unit the time unit
     * @return this
     * @see #withIssuedAt(Instant)
     */
    public MockTicketProvider withAccessExpiration(long time, @Nonnull TimeUnit unit) {
        Objects.requireNonNull(unit);
        accessExpirationSec = toSec(time, unit);
        return this;
    }

    @Override
    public Ticket restore(@Nonnull String text) {
        Objects.requireNonNull(text);
        var token = JWT.decode(text);
        if (!Objects.equals(token.getIssuer(), CLAIM_ISSUER)) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "invalid issuer: \"{0}\" (expect: \"{1}\")",
                    token.getIssuer(),
                    CLAIM_ISSUER));
        }
        TokenKind kind = null;
        if (Objects.equals(token.getSubject(), CLAIM_SUBJECT_ACCESS)) {
            kind = TokenKind.ACCESS;
        } else if (Objects.equals(token.getSubject(), CLAIM_SUBJECT_REFRESH)) {
            kind = TokenKind.REFRESH;
        } else {
            throw new IllegalArgumentException(MessageFormat.format(
                    "invalid issuer: \"{0}\" (expect: \"{1}\" or \"{2}\")",
                    token.getSubject(),
                    CLAIM_SUBJECT_ACCESS,
                    CLAIM_SUBJECT_REFRESH));
        }
        if (token.getAudience() == null || token.getAudience().size() != 1) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "invalid audience: {0} (expect: just one authenticated user)",
                    token.getAudience()));
        }
        String name = token.getAudience().get(0);
        if (token.getIssuedAt() == null) {
            throw new IllegalArgumentException("issued_at must be set");
        }
        if (token.getExpiresAt() == null) {
            throw new IllegalArgumentException("expires_at must be set");
        }
        return new JwtTicket(this, name, name, Map.of(kind, token));
    }

    /**
     * Obtains a new {@link Ticket} using a pair of user ID and its password.
     * <p>
     * Before invoke this, you must register user name and password by using {@link #withUser(String, String)
     * withUser()}.
     * If the specified user name and password have not been added, this will raise {@link CoreServiceException}
     * with {@link CoreServiceCode#AUTHENTICATION_ERROR}.
     * </p>
     * <p>
     * The returned ticket will have the following properties:
     *
     * <ul>
     * <li>
     *      The ticket contains a refresh token if and only if {@link #withRefreshExpiration(long, TimeUnit) refresh
     *      expiration} is a positive value
     * </li>
     * <li>
     *      The ticket contains an access token if and only if {@link #withAccessExpiration(long, TimeUnit) access
     *      expiration} is a positive value
     * </li>
     * <li>
     *      The individual tokens will have the expiration date based on the time which is set in {@link #withIssuedAt(Instant)}.
     * </li>
     * </ul>
     *
     * </p>
     * @see #withUser(String, String)
     * @see #withRefreshExpiration(long, TimeUnit)
     * @see #withAccessExpiration(long, TimeUnit)
     */
    @Override
    public Ticket issue(@Nonnull String userId, @Nonnull String password) throws IOException, CoreServiceException {
        Objects.requireNonNull(userId);
        Objects.requireNonNull(password);
        if (!Objects.equals(credentials.get(userId), password)) {
            throw new CoreServiceException(CoreServiceCode.AUTHENTICATION_ERROR, "invalid user name or password");
        }
        var tokens = new EnumMap<TokenKind, DecodedJWT>(TokenKind.class);
        if (refreshExpirationSec >= 0) {
            tokens.put(TokenKind.REFRESH, buildToken(TokenKind.REFRESH, userId, refreshExpirationSec));
        }
        if (accessExpirationSec >= 0) {
            tokens.put(TokenKind.ACCESS, buildToken(TokenKind.ACCESS, userId, accessExpirationSec));
        }
        return new JwtTicket(this, userId, userId, tokens);
    }

    /**
     * Requests to extend access expiration time of the given {@link Ticket}.
     * <p>
     * If the refresh token in the ticket was already expired, this will raise {@link CoreServiceException}
     * with {@link CoreServiceCode#REFRESH_EXPIRED}.
     * Note that the current time (not {@link #withIssuedAt(Instant)}) is used to verify the expiration date.
     * </p>
     * <p>
     * After this operation was succeeded, the returned ticket will contain an access token.
     * The access token will have the expiration date based on the time which is set in {@link #withIssuedAt(Instant)}.
     * </p>
     */
    @Override
    public Ticket refresh(
            @Nonnull Ticket ticket,
            long expiration,
            @Nonnull TimeUnit unit) throws IOException, CoreServiceException {
        Objects.requireNonNull(ticket);
        Objects.requireNonNull(unit);
        if (!(ticket instanceof JwtTicket)) {
            throw new IllegalArgumentException("ticket is inconsistent for this provider");
        }
        JwtTicket jwt = (JwtTicket) ticket;
        if (jwt.getProvider() != this) {
            throw new IllegalArgumentException("ticket is not issued by this provider");
        }
        if (jwt.getRefreshExpirationTime().isEmpty()) {
            throw new IllegalArgumentException("ticket does not contain a refresh token");
        }
        if (jwt.getRefreshExpirationTime().get().isBefore(Instant.now())) {
            throw new CoreServiceException(CoreServiceCode.REFRESH_EXPIRED, "refresh token was expired");
        }
        String name = ticket.getUserId();
        var access = buildToken(TokenKind.ACCESS, name, toSec(expiration, unit));
        return jwt.merge(
                TokenKind.ACCESS,
                new JwtTicket(this, name, name, Map.of(TokenKind.ACCESS, access)));
    }

    private static long toSec(long time, TimeUnit unit) {
        if (time < 0) {
            return -1;
        }
        return unit.toSeconds(time);
    }

    private Instant issuedAt() {
        if (issued != null) {
            return issued;
        }
        return Instant.now();
    }

    private DecodedJWT buildToken(TokenKind kind, String user, long expiration) {
        var now = issuedAt();
        var token = JWT.create()
            .withIssuer(CLAIM_ISSUER)
            .withSubject(CLAIM_SUBJECT.get(kind))
            .withAudience(user)
            .withIssuedAt(Date.from(now))
            .withExpiresAt(Date.from(now.plus(Duration.ofSeconds(expiration))))
            .sign(Algorithm.none());
        return JWT.decode(token);
    }
}
