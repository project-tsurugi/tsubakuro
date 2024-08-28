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
