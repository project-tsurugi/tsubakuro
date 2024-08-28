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
package com.tsurugidb.tsubakuro.auth;

import java.time.Instant;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;

/**
 * Provides authentication information.
 */
public interface Ticket {

    /**
     * Returns the token text.
     * <p>
     * This may return appropriate token - access token or refresh token.
     * </p>
     * @return the token text
     * @see #getToken(TokenKind)
     * @see TicketProvider#restore(String)
     */
    String getToken();

    /**
     * Returns the token text of the specified kind.
     * @param kind the token kind
     * @return the token text of the specified, or empty if this ticket does not include such the kind
     */
    default Optional<String> getToken(@Nonnull TokenKind kind) {
        return Optional.of(getToken());
    }

    /**
     * Returns the authenticated user ID.
     * @return the authenticated user ID
     */
    String getUserId();

    /**
     * Returns the authenticated user name.
     * <p>
     * This is sometimes equivalent to {@link #getUserId() the user ID}.
     * </p>
     * @return the authenticated user name
     */
    default String getUserName() {
        return getUserId();
    }

    /**
     * Returns expiration time of access to the permitted resources.
     * @return the access expiration time, or empty if any access is not permitted in this ticket
     */
    default Optional<Instant> getAccessExpirationTime() {
        return Optional.empty();
    }

    /**
     * Returns expiration time of refresh this ticket itself.
     * @return the refresh expiration time, or empty if refresh is not permitted in this ticket
     */
    default Optional<Instant> getRefreshExpirationTime() {
        return Optional.empty();
    }

    /**
     * Returns credential information of this ticket.
     * <p>
     * This may return appropriate credential - access token or refresh token.
     * </p>
     * @return the credential information
     */
    default RememberMeCredential toCredential() {
        return new RememberMeCredential(getToken());
    }

    /**
     * Returns the token text of the specified kind.
     * @param kind the token kind
     * @return the token text of the specified, or empty if this ticket does not include such the kind
     */
    default Optional<RememberMeCredential> toCredential(@Nonnull TokenKind kind) {
        return getToken(kind).map(RememberMeCredential::new);
    }
}
