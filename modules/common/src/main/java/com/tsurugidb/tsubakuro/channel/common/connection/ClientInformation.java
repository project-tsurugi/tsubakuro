/*
 * Copyright 2023-2026 Project Tsurugi.
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
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.tsubakuro.common.BlobTransferType;

/**
 * A client information intened to be used in handshake.
 */
public final class ClientInformation {

    private final String connectionLabel;

    private final String applicationName;

    private final Credential credential;

    private final BlobTransferType blobTransferType;

    /**
     * Creates a new instance.
     * @param connectionLabel the label.
     * @param applicationName the application name.
     * @param credential the connection credential.
     * @param type the blob transfer type.
     */
    public ClientInformation(@Nullable String connectionLabel, @Nullable String applicationName, @Nonnull Credential credential, @Nonnull BlobTransferType type) {
        Objects.requireNonNull(credential);
        Objects.requireNonNull(type);
        this.connectionLabel = connectionLabel;
        this.applicationName = applicationName;
        this.credential = credential;
        this.blobTransferType = type;
    }

    /**
     * Creates a new instance without information.
     * This exists for testing purposes.
     */
    public ClientInformation() {
        this(null, null, NullCredential.INSTANCE, BlobTransferType.DEFAULT);
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

    /**
     * Get the blob transfer media list.
     * @return the blob transfer media list, empty if no blob transfer media has been set.
     */
    public List<EndpointRequest.BlobTransferMedium> getBlobTransferMedia() {
        switch (blobTransferType) {
            case DOES_NOT_USE:
                return new ArrayList<>(List.of(
                        EndpointRequest.BlobTransferMedium.newBuilder().setBlobTransferType(EndpointRequest.BlobTransferType.DOES_NOT_USE).build()
                ));
            case PRIVILEGED:
                return new ArrayList<>(List.of(
                        EndpointRequest.BlobTransferMedium.newBuilder().setBlobTransferType(EndpointRequest.BlobTransferType.PRIVILEGED).build()
                ));
            case RELAY:
                return new ArrayList<>(List.of(
                        EndpointRequest.BlobTransferMedium.newBuilder().setBlobTransferType(EndpointRequest.BlobTransferType.RELAY).build()
                ));
            case DEFAULT:
                return new ArrayList<>(List.of(
                        EndpointRequest.BlobTransferMedium.newBuilder().setBlobTransferType(EndpointRequest.BlobTransferType.RELAY).build(),
                        EndpointRequest.BlobTransferMedium.newBuilder().setBlobTransferType(EndpointRequest.BlobTransferType.DOES_NOT_USE).build()
                ));
            default:
                throw new IllegalArgumentException("Unsupported BlobTransferType: " + blobTransferType);
        }
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ClientInformation(connectionLabel={0}, applicationName={1}, credential={2}, blobTransferType={3})",
                checkNull(connectionLabel), checkNull(applicationName), credential.toString(), blobTransferType.toString());
    }
    private String checkNull(String string) {
        return (string != null) ? string : "";
    }

}