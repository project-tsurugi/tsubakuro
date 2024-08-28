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
package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * An authenticated token from authority center.
 */
public class RememberMeCredential implements Credential {

    private final String token;

    /**
     * Creates a new instance.
     * @param token the authenticated token string
     */
    public RememberMeCredential(@Nonnull String token) {
        Objects.requireNonNull(token);
        this.token = token;
    }

    /**
     * Returns the token text.
     * @return the token text
     */
    public String getToken() {
        return token;
    }

    @Override
    public String toString() {
        return "RememberMeCredential()";
    }
}
