package com.tsurugidb.tsubakuro.auth.http;

import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum MessageType {

    OK,

    AUTH_ERROR,

    NO_TOKEN,

    TOKEN_EXPIRED,

    INVALID_AUDIENCE,

    INVALID_TOKEN,

    UNKNOWN,
    ;

    private static final Logger LOG = LoggerFactory.getLogger(MessageType.class);

    static MessageType deserialize(@Nonnull String text) {
        Objects.requireNonNull(text);
        try {
            return valueOf(text.toUpperCase(Locale.ENGLISH));
        } catch (NoSuchElementException e) {
            LOG.debug("unhandled message type", e);
            return UNKNOWN;
        }
    }

    String serialize() {
        return name().toLowerCase(Locale.ENGLISH);
    }
}
