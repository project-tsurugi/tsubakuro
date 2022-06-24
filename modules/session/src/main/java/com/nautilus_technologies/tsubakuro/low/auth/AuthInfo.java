package com.nautilus_technologies.tsubakuro.low.auth;

import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import  com.nautilus_technologies.tsubakuro.channel.common.connection.RememberMeCredential;

/**
 * Represents authentication information.
 */
public class AuthInfo {

    private final String name;

    private final RememberMeCredential token;

    /**
     * Creates a new instance.
     * @param name the user name
     * @param token the authenticated token.
     */
    public AuthInfo(@Nonnull String name, @Nonnull RememberMeCredential token) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(token);
        this.name = name;
        this.token = token;
    }

    /**
     * Returns the user name.
     * @return the user name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the authentication token.
     * @return the authentication token.
     */
    public RememberMeCredential getToken() {
        return token;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "AuthInfo(name={0})",
                name);
    }
}
