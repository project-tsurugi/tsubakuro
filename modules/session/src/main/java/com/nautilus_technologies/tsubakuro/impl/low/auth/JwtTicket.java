package com.nautilus_technologies.tsubakuro.impl.low.auth;

import java.time.Instant;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.nautilus_technologies.tsubakuro.low.auth.Ticket;
import com.nautilus_technologies.tsubakuro.low.auth.TicketProvider;
import com.nautilus_technologies.tsubakuro.low.auth.TokenKind;

/**
 * An implementation of {@link Ticket} that represents by JWT.
 */
public class JwtTicket implements Ticket {

    private final TicketProvider provider;

    private final String userId;

    private final String userName;

    private final Map<TokenKind, DecodedJWT> tokens = new EnumMap<>(TokenKind.class);

    /**
     * Creates a new instance.
     * @param provider the source provider
     * @param userId the user ID
     * @param userName the user name
     * @param tokens the individual auth tokens
     * @see #fromToken(TicketProvider, String, Function, Function, Function)
     */
    public JwtTicket(
            @Nullable TicketProvider provider,
            @Nonnull String userId,
            @Nullable String userName,
            @Nonnull Map<? extends TokenKind, ? extends DecodedJWT> tokens) {
        Objects.requireNonNull(userId);
        Objects.requireNonNull(tokens);
        this.provider = provider;
        this.userId = userId;
        this.userName = Objects.requireNonNullElse(userName, userId);
        this.tokens.putAll(tokens);
    }

    /**
     * Parses a JWT text and returns the corresponded ticket.
     * @param provider the source provider
     * @param token the JWT text
     * @param tokenKind the token kind picker from the JWT
     * @param userId the user ID picker from JWT
     * @param userName the user name picker from JWT
     * @return the corresponded ticket object
     * @throws JWTDecodeException if token text is something wrong
     */
    public static JwtTicket fromToken(
            @Nullable TicketProvider provider,
            @Nonnull String token,
            @Nonnull Function<? super DecodedJWT, TokenKind> tokenKind,
            @Nonnull Function<? super DecodedJWT, String> userId,
            @Nullable Function<? super DecodedJWT, String> userName) throws JWTDecodeException {
        Objects.requireNonNull(userId);
        Objects.requireNonNull(token);
        Objects.requireNonNull(tokenKind);
        Objects.requireNonNull(userId);
        var jwt = JWT.decode(token);
        var kind = tokenKind.apply(jwt);
        if (kind == null) {
            throw new JWTDecodeException("failed to extract token kind");
        }
        var id = userId.apply(jwt);
        if (id == null) {
            throw new JWTDecodeException("failed to extract user ID");
        }
        var name = Optional.ofNullable(userName)
                .flatMap(it -> Optional.ofNullable(it.apply(jwt)))
                .orElse(id);
        return new JwtTicket(provider, id, name, Map.of(kind, jwt));
    }

    /**
     * Merges this and the given ticket.
     * <p>
     * This operation does not change this object.
     * THe created ticket has the same provider, user ID, and user name as this ticket.
     * </p>
     * @param kind the token kind to merge in the given ticket
     * @param other the ticket to merge
     * @return the merged ticket
     */
    public JwtTicket merge(@Nonnull TokenKind kind, @Nonnull JwtTicket other) {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(other);
        var newTokens = new EnumMap<TokenKind, DecodedJWT>(TokenKind.class);
        getJwt(opposite(kind)).ifPresent(it -> newTokens.put(opposite(kind), it));
        other.getJwt(kind).ifPresent(it -> newTokens.put(kind, it));
        return new JwtTicket(provider, userId, userName, newTokens);
    }

    private static TokenKind opposite(@Nonnull TokenKind kind) {
        assert kind != null;
        if (kind == TokenKind.ACCESS) {
            return TokenKind.REFRESH;
        }
        return TokenKind.ACCESS;
    }

    /**
     * Returns the {@link TicketProvider} which provides this ticket.
     * @return the ticket provider of this
     */
    public TicketProvider getProvider() {
        return provider;
    }

    /**
     * Returns the JWT object for the kind.
     * @param kind the token kind
     * @return the JWT object, or empty if this ticket does not contain such the kind
     */
    public Optional<DecodedJWT> getJwt(@Nonnull TokenKind kind) {
        assert kind != null;
        return Optional.ofNullable(tokens.get(kind));
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public Optional<Instant> getAccessExpirationTime() {
        return getJwt(TokenKind.ACCESS)
                .map(DecodedJWT::getExpiresAt)
                .map(Date::toInstant);
    }

    @Override
    public Optional<Instant> getRefreshExpirationTime() {
        return getJwt(TokenKind.REFRESH)
                .map(DecodedJWT::getExpiresAt)
                .map(Date::toInstant);
    }

    @Override
    public String getToken() {
        return getToken(TokenKind.REFRESH)
                .or(() -> getToken(TokenKind.ACCESS))
                .orElseThrow();
    }

    @Override
    public Optional<String> getToken(@Nonnull TokenKind kind) {
        Objects.requireNonNull(kind);
        return getJwt(kind)
                .map(DecodedJWT::getToken);
    }
}
