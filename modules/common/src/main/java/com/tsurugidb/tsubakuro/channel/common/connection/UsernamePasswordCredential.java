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

import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A credential by user name and password.
 */
public class UsernamePasswordCredential implements Credential {

    private final String name;

    private final String password;

    /**
     * Creates a new instance.
     * @param name the user name
     * @param password the password
     */
    public UsernamePasswordCredential(@Nonnull String name, @Nullable String password) {
        Objects.requireNonNull(name);
        this.name = name;
        this.password = password;
    }

    /**
     * Returns the user name.
     * @return the user name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the password.
     * @return the password
     */
    public Optional<String> getPassword() {
        return Optional.of(password);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "UsernamePasswordCredential(name={0})",
                name);
    }
}
