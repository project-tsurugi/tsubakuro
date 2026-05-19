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
package com.tsurugidb.tsubakuro.common.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.FileInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Optional;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.blob_relay_privilege.proto.BlobRelayPrivilegeRequest;
import com.tsurugidb.blob_relay_privilege.proto.BlobRelayPrivilegeResponse;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.ForegroundFutureResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.BlobPathMapping;
import com.tsurugidb.tsubakuro.common.LargeObjectCache;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.common.LargeObjectReference;
import com.tsurugidb.tsubakuro.common.exception.BlobException;
import com.tsurugidb.tsubakuro.client.ServiceClient;
import com.tsurugidb.tsubakuro.client.ServiceMessageVersion;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link LargeObjectClient} that provides privileged access to the Large Object storage.
 * This client is used when the endpoint supports privileged access and the session is configured to use privileged transfer type.
 *
 * @since 1.11.0
 */
public class LargeObjectClientPrivileged implements LargeObjectClient {
    /**
     * A dummy ServiceClient for exposing service message version of current common blob_relay_privilege implementations.
     */
    @ServiceMessageVersion(
            service = LargeObjectClientPrivileged.BLOB_RELAY_PRIVILEGE_SERVICE_SYMBOLIC_ID,
            major = LargeObjectClientPrivileged.BLOB_RELAY_PRIVILEGE_SERVICE_MESSAGE_VERSION_MAJOR,
            minor = LargeObjectClientPrivileged.BLOB_RELAY_PRIVILEGE_SERVICE_MESSAGE_VERSION_MINOR)
    public static class BlobRelayPrivilegeClient implements ServiceClient {
        // no special members
    }

    /**
     * The symbolic ID of this implementation.
     */
    static final String BLOB_RELAY_PRIVILEGE_SERVICE_SYMBOLIC_ID = "blob_relay_privilege"; //$NON-NLS-1$

    /**
     * The major service message version for FrameworkRequest.Header.
     */
    static final int BLOB_RELAY_PRIVILEGE_SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version for FrameworkRequest.Header.
     */
    static final int BLOB_RELAY_PRIVILEGE_SERVICE_MESSAGE_VERSION_MINOR = 0;

    /**
     * The service id for endpoint broker.
     */
    private static final int SERVICE_ID_BLOB_RELAY_PRIVILEGE = 13;

    static final Logger LOG = LoggerFactory.getLogger(LargeObjectClientPrivileged.class);

    private final Wire wire;
    private final BlobPathMapping blobPathMapping;

    /**
     * Creates a new instance.
     * @param wire the wire for communication with the server
     * @param blobPathMapping the blob path mapping
     */
    public LargeObjectClientPrivileged(@Nonnull Wire wire, @Nullable BlobPathMapping blobPathMapping) {
        this.wire = wire;
        this.blobPathMapping = blobPathMapping;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(InputStream source) throws BlobException {
        // Implementation for privileged mode upload from InputStream
        return null;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Reader source) throws BlobException {
        // Implementation for privileged mode upload from Reader
        return null;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Path source) throws IOException {
        var info = SqlRequest.ClientOnlyLargeObjectInfo.newBuilder();
        if (blobPathMapping != null) {
            var mapping = blobPathMapping.getOnSend();
            if (mapping != null) {
                for (var entry : mapping) {
                    var cp = entry.getClientPath();
                    var clientPath = cp.isAbsolute() ? cp : cp.toAbsolutePath();
                    if (source.startsWith(clientPath)) {
                        var remainingPath = source.subpath(entry.getClientPath().getNameCount(), source.getNameCount());
                        if (remainingPath != null) {
                            String serverPath = entry.getServerPath();
                            for (int i = 0; i < remainingPath.getNameCount(); i++) {
                                serverPath += "/" + remainingPath.getName(i).toString();  // server path is separated by "/"
                            }
                            info.setServerPath(serverPath);
                            LargeObjectInfo largeObjectInfo = new LargeObjectInfoImpl(info.build());
                            return FutureResponse.returns(largeObjectInfo);
                        }
                    }
                }
            }
        }        // If no mapping is found, treat the path as a server path directly
        info.setServerPath(source.toString());
        LargeObjectInfo largeObjectInfo = new LargeObjectInfoImpl(info.build());
        return FutureResponse.returns(largeObjectInfo);
    }

    private class OpenInputStreamProcessor implements MainResponseProcessor<InputStream> {
        @Override
        public InputStream process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = BlobRelayPrivilegeResponse.GetBlob.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var serverPath = message.getSuccess().getServerFilePath();
                final Path clientPath = applyBlobPathMapping(serverPath, blobPathMapping);
                if (!Files.exists(clientPath)) {
                    throw new BlobException("BLOB file does not exist.");
                }
                return new FileInputStream(clientPath.toFile());
            case ERROR:
                var err = message.getError();
                throw new CoreServiceException(CoreServiceCode.valueOf(err.getCode()), err.getMessage());
            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<InputStream> openInputStream(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        try {
            FutureResponse<? extends Response> future = wire.send(
                SERVICE_ID_BLOB_RELAY_PRIVILEGE,
                toDelimitedByteArray(newRequest(contextId, ref))
            );
            return new ForegroundFutureResponse<>(future, new OpenInputStreamProcessor().asResponseProcessor(), null);
        } catch (IOException e) {
            throw new BlobException(e);
        }
    }

    @Override
    public FutureResponse<Reader> openReader(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        try {
            var inputStreamReader = new InputStreamReader(openInputStream(contextId, ref).await(), StandardCharsets.UTF_8);
            return FutureResponse.returns(inputStreamReader);
        } catch (IOException | InterruptedException | ServerException e) {
            throw new BlobException(e);
        }
    }

    @Override
    public FutureResponse<LargeObjectCache> getLargeObjectCache(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        try {
            return blobRelayPrivilege(contextId, ref);
        } catch (IOException e) {
            throw new BlobException(e);
        }
    }

    @Override
    public FutureResponse<Void> copyTo(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref, @Nonnull Path destination) throws BlobException {
        Objects.requireNonNull(contextId);
        Objects.requireNonNull(ref);
        Objects.requireNonNull(destination);
        try {
            var largeObjectCache = blobRelayPrivilege(contextId, ref).await();
            var optionalPath = largeObjectCache.find();
            if (optionalPath.isEmpty()) {
                throw new BlobException("Server file path is empty.");
            }
            Files.copy(optionalPath.get(), destination);
            return FutureResponse.returns(null);
        } catch (IOException | InterruptedException | ServerException e) {
            throw new BlobException(e);
        }
    }

    private class BlobRelayPrivilegeProcessor implements MainResponseProcessor<LargeObjectCache> {
        @Override
        public LargeObjectCache process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = BlobRelayPrivilegeResponse.GetBlob.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var serverPath = message.getSuccess().getServerFilePath();
                final Path clientPath = applyBlobPathMapping(serverPath, blobPathMapping);
                return new LargeObjectCache() {
                    private final Path path = clientPath;
                    private final boolean exists = Files.exists(clientPath);
                    private final AtomicBoolean closed = new AtomicBoolean();

                    @Override
                    public Optional<Path> find() {
                        if (closed.get() || !exists) {
                            return Optional.empty();
                        }
                        return Optional.of(path);
                    }
                    @Override
                    public void close() {
                        closed.set(true);
                    }
                };
            case ERROR:
                var err = message.getError();
                throw new CoreServiceException(CoreServiceCode.valueOf(err.getCode()), err.getMessage());
            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    /**
     * Performs blobRelayPrivilege process.
     * @param transactionId the transaction ID
     * @param largeObjectReference the large object reference
     * @return a future response that will complete with the server file path if the process is successful, or an empty optional if the server file path is empty
     * @throws IOException if an I/O error occurs during the blobRelayPrivilege process
     */
    private FutureResponse<LargeObjectCache> blobRelayPrivilege(@Nonnull ContextId contextId, @Nonnull LargeObjectReference largeObjectReference) throws IOException {
        FutureResponse<? extends Response> future = wire.send(
            SERVICE_ID_BLOB_RELAY_PRIVILEGE,
            toDelimitedByteArray(newRequest(contextId, largeObjectReference))
        );
        return new ForegroundFutureResponse<>(future, new BlobRelayPrivilegeProcessor().asResponseProcessor(), null);
    }

    private BlobRelayPrivilegeRequest.Request newRequest(ContextId contextId, LargeObjectReference ref) {
        return BlobRelayPrivilegeRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(BLOB_RELAY_PRIVILEGE_SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(BLOB_RELAY_PRIVILEGE_SERVICE_MESSAGE_VERSION_MINOR)
                .setGetBlob(BlobRelayPrivilegeRequest.GetBlob.newBuilder()
                    .setTransactionHandle(contextId.getTransactionHandle())
                    .setBlobReference(BlobRelayPrivilegeRequest.BlobReference.newBuilder()
                            .setStorageId(ref.getProvider())
                            .setObjectId(ref.getObjectId())
                            .setTag(ref.getReferenceTag())))
                .build();
    }

    private static byte[] toDelimitedByteArray(BlobRelayPrivilegeRequest.Request message) throws IOException {
        try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
            message.writeDelimitedTo(byteArrayOutputStream);
            return byteArrayOutputStream.toByteArray();
        }
    }

    private static Path applyBlobPathMapping(String path, BlobPathMapping blobPathMapping) {
        if (blobPathMapping != null) {
            Path filePath  = null;
            String serverFileName = path.startsWith("/") ? path : "/" + path;
            String[] serverFileNameElements = serverFileName.split("/");

            outerloop: for (var m : blobPathMapping.getOnReceive()) {
                String msp = m.getServerPath();
                String serverPath = msp.startsWith("/") ? msp : "/" + msp;
                String[] serverPathElements = serverPath.split("/");
                int length = serverPathElements.length;
                if (serverFileNameElements.length < length) {
                    continue;
                }
                for (int i = 0; i < length; i++) {
                    if (!serverFileNameElements[i].equals(serverPathElements[i])) {
                        continue outerloop;
                    }
                }
                filePath = m.getClientPath();
                for (int i = length; i < serverFileNameElements.length; i++) {
                    filePath = filePath.resolve(serverFileNameElements[i]);
                }
                return filePath;
            }
        }
        return Path.of(path);
    }
}