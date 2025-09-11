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

import java.io.IOException;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;

/**
 * A credential by user name and password.
 */
public class UsernamePasswordCredential implements Credential {

    private final String name;

    private final String password;

    /**
     * The format version field name in credential file.
     */
    public static final String KEY_FORMAT_VERSION = "format_version"; //$NON-NLS-1$

    /**
     * The encrypted user name field name in credential file.
     */
    public static final String KEY_USER = "user"; //$NON-NLS-1$

    /**
     * The encrypted password field name in credential file.
     */
    public static final String KEY_PASSWORD = "password"; //$NON-NLS-1$

    /**
     * The expiration date field name in credential file.
     */
    public static final String KEY_EXPIRATION_DATE = "expiration_date"; //$NON-NLS-1$

    /**
     * The current format version.
     */
    private static final int FORMAT_VERSION = 1;

    /**
     * The maximum name length.
     */
    public static final int MAXIMUM_NAME_LENGTH = 60;  // FIXME alter to 1024 as the specification defined

    /**
     * The maximum password length.
     */
    public static final int MAXIMUM_PASSWORD_LENGTH = 60;  // FIXME alter to 1024 as the specification defined


    private static final JsonFactory JSON = new JsonFactoryBuilder()
            .build();

    private static final int INITIAL_CAPACITY_FOR_JSON_STRING_WRITER = 256;

    /**
     * Creates a new instance.
     * @param name the user name
     * @param password the password
     */
    public UsernamePasswordCredential(@Nonnull String name, @Nullable String password) {
        Objects.requireNonNull(name);
        this.name = name;
        this.password = password;
        if (this.name.length() > MAXIMUM_NAME_LENGTH) {
            throw new IllegalArgumentException("name is too long"); //$NON-NLS-1$
        }
        if (this.password != null && this.password.length() > MAXIMUM_PASSWORD_LENGTH) {
            throw new IllegalArgumentException("password is too long"); //$NON-NLS-1$
        }
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
        return Optional.ofNullable(password);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "UsernamePasswordCredential(name={0})",
                name);
    }

    /**
     * Returns the JSON representation of this credential.
     * @param dueInstant the expiration date of the credential, or null if no expiration date is set
     * @return the JSON string
     * @throws IOException error occurred in write json to a StringWriter
     */
    public String getJsonText(Instant dueInstant) throws IOException {
        StringWriter stringWriter = new StringWriter(INITIAL_CAPACITY_FOR_JSON_STRING_WRITER);
        try (var writer = JSON.createGenerator(stringWriter)) {
            writer.writeStartObject();
            writer.writeFieldName(KEY_FORMAT_VERSION);
            writer.writeNumber(FORMAT_VERSION);
            writer.writeFieldName(KEY_USER);
            writer.writeString(name);
            writer.writeFieldName(KEY_PASSWORD);
            writer.writeString(password != null ? password : "");
            if (dueInstant != null) {
                writer.writeFieldName(KEY_EXPIRATION_DATE);
                writer.writeString(dueInstant.truncatedTo(ChronoUnit.MICROS).toString());
            }
            writer.writeEndObject();
        }
        return stringWriter.toString();
    }
}