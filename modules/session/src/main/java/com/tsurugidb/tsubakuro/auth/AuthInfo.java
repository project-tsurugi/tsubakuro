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

import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;

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
