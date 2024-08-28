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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A client information intened to be used in handshake.
 */
public final class ClientInformation {

    private final String connectionLabel;

    private final String applicationName;

    private final Credential credential;

    /**
     * Creates a new instance without information.
     */
    public ClientInformation() {
        this.connectionLabel = null;
        this.applicationName = null;
        this.credential = NullCredential.INSTANCE;
    }

    /**
     * Creates a new instance.
     * @param connectionLabel the label.
     * @param applicationName the application name.
     * @param credential the connection credential.
     */
    public ClientInformation(@Nullable String connectionLabel, @Nullable String applicationName, @Nonnull Credential credential) {
        Objects.requireNonNull(credential);
        this.connectionLabel = connectionLabel;
        this.applicationName = applicationName;
        this.credential = credential;
    }

    /**
     * Get the connection label.
     * @return the connection label, null if connection label has not been set.
     */
    public String getConnectionLabel() {
        return connectionLabel;
    }

    /**
     * Get the application name.
     * @return the application name, null if application name has not been set.
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Get the credential.
     * @return the connection credential.
     */
    public Credential getCredential() {
        return credential;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ClientInformation(connectionLabel={0}, applicationName={1}, credential={2})",
                checkNull(connectionLabel), checkNull(applicationName), credential.toString());
    }
    private String checkNull(String string) {
        return (string != null) ? string : "";
    }

}
