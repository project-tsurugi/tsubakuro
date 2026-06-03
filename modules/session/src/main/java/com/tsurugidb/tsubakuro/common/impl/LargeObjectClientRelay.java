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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;
import com.tsurugidb.tsubakuro.common.BlobRelayReference;
import com.tsurugidb.tsubakuro.common.LargeObjectCache;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.common.LargeObjectReference;
import com.tsurugidb.tsubakuro.common.exception.BlobException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.relay.client.BlobRelayStreaming;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link LargeObjectClient} that provides privileged access to the Large Object storage.
 * This client is used when the endpoint supports privileged access and the session is configured to use privileged transfer type.
 *
 * @since 1.11.0
 */
public class LargeObjectClientRelay implements LargeObjectClient {
    private static final long API_VERSION = 1;

    private static final Logger LOG = LoggerFactory.getLogger(LargeObjectClientRelay.class);

    private final long sessionId;
    private final String endpoint;
    private final boolean secure;
    private final long chunkSize;
    private volatile BlobRelayStreaming blobRelayStreaming = null;

    /**
     * Creates a new instance.
     * @param sessionId the session ID for the BlobRelayStreaming
     * @param endpoint the endpoint for the BlobRelayStreaming
     * @param secure whether to use secure connection for the BlobRelayStreaming
     * @param chunkSize the chunk size for the BlobRelayStreaming
     */
    public LargeObjectClientRelay(@Nonnull String sessionId, @Nonnull String endpoint, boolean secure, long chunkSize) {
        try {
            this.sessionId = Long.parseLong(sessionId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid session ID: " + sessionId, e);
        }
        this.endpoint = endpoint;
        this.secure = secure;
        this.chunkSize = validateChunkSize(chunkSize);
    }
    private static long validateChunkSize(long chunkSize) {
        if (chunkSize <= 0 || chunkSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("chunkSize must be between 1 and " + Integer.MAX_VALUE + ": " + chunkSize);
        }
        return chunkSize;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(InputStream source) throws BlobException {
        return internalUpload(Streaming.PutStreamingRequest.Metadata.newBuilder()
                                .setApiVersion(API_VERSION)
                                .setSessionId(sessionId)
                                .build(),
                              source);
    }
    private FutureResponse<LargeObjectInfo> internalUpload(Streaming.PutStreamingRequest.Metadata meta, InputStream source) throws BlobException {
        final AtomicReference<FutureResponse<BlobRelayCommon.BlobReference>> reference = new AtomicReference<>();

        try {
            openBlobRelayStreaming();
            reference.set(blobRelayStreaming.put(meta, source));
            return new FutureResponse<LargeObjectInfo>() {
                @Override
                public LargeObjectInfo get() throws IOException, InterruptedException, ServerException {
                    try {
                        return get(0, null);
                    } catch (TimeoutException e) {
                        throw new AssertionError("Unexpected timeout", e);
                    }
                }
                @Override
                public LargeObjectInfo get(long timeout, TimeUnit unit) throws IOException, InterruptedException, ServerException, TimeoutException {
                    var ref = reference.get().get(timeout, unit);
                    return new LargeObjectInfo() {
                        @Override
                        public InfoType getInfoType() {
                            return LargeObjectInfo.InfoType.BLOB_RELAY_REFERENCE;
                        }
                        @Override
                        public BlobRelayReference getBlobRelayReference() {
                            return new BlobRelayReference(ref.getStorageId(), ref.getObjectId(), ref.getTag());
                        }
                        @Override
                        public String getServerPath() {
                            throw new IllegalStateException("LargeObjectInfo type is BLOB_RELAY_REFERENCE, server path is not available");
                        }
                    };
                }
                @Override
                public boolean isDone() {
                    return reference.get().isDone();
                }
                @Override
                public void close() {
                    // do nothing, the caller is responsible for closing the input stream and the BlobRelayStreaming will be closed when this client is closed
                }
            };
        } catch (IOException | InterruptedException e) {
            throw new BlobException("Failed to open BlobRelayStreaming", e);
        }
    }
    private synchronized void openBlobRelayStreaming() throws IOException {
        if (blobRelayStreaming == null) {
            blobRelayStreaming = new BlobRelayStreaming(endpoint, secure, chunkSize);
        }
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Reader source) throws BlobException {
        try {
            return upload(convertInputStream(source, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new BlobException("Failed to read from Reader", e);
        }
    }
    InputStream convertInputStream(Reader reader, Charset charset) throws IOException {
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, (int) chunkSize);

        new Thread(() -> {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(pos, charset))) {
                char[] buffer = new char[(int) chunkSize];
                int len;
                while ((len = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, len);
                }
                writer.flush();
            } catch (IOException e) {
                LOG.error("Error while converting Reader to InputStream", e);
            } finally {
                try {
                    pos.close();
                } catch (IOException e) {
                    LOG.error("Error while closing PipedOutputStream", e);
                }
            }
        }).start();

        return pis;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Path source) throws BlobException {
        try {
            return internalUpload(Streaming.PutStreamingRequest.Metadata.newBuilder()
                                    .setApiVersion(API_VERSION)
                                    .setSessionId(sessionId)
                                    .setBlobSize(Files.size(source))
                                    .build(),
                                  Files.newInputStream(source));
        } catch (IOException e) {
            throw new BlobException("Failed to read from Path", e);
        }
    }

    @Override
    public FutureResponse<InputStream> openInputStream(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        try {
            openBlobRelayStreaming();
            return blobRelayStreaming.get(newGetStreamingRequest(contextId, ref));
        } catch (IOException | InterruptedException e) {
            throw new BlobException("Failed to open InputStream", e);
        }
    }
    private Streaming.GetStreamingRequest newGetStreamingRequest(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) {
        return Streaming.GetStreamingRequest.newBuilder()
                    .setApiVersion(API_VERSION)
                    .setTransactionId(contextId.getTransactionHandle())
                    .setBlob(BlobRelayCommon.BlobReference.newBuilder()
                            .setStorageId(toStorageId(ref.getProvider()))
                            .setObjectId(ref.getObjectId())
                            .setTag(ref.getReferenceTag())
                            .build())
                    .build();
    }

    @Override
    public FutureResponse<Reader> openReader(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        return new FutureResponse<Reader>() {
            @Override
            public Reader get() throws IOException, InterruptedException, ServerException {
                 try {
                    return get(0, null);
                } catch (TimeoutException e) {
                    throw new AssertionError("Unexpected timeout", e);
                }
            }
            public Reader get(long timeout, TimeUnit unit) throws IOException, InterruptedException, ServerException, TimeoutException {
                var inputStreamFuture = openInputStream(contextId, ref);
                try {
                    return new InputStreamReader(inputStreamFuture.get(timeout, unit), StandardCharsets.UTF_8);
                } catch (IOException | InterruptedException | ServerException | TimeoutException e) {
                    throw new BlobException("Failed to open Reader", e);
                }
            }
            @Override
            public boolean isDone() {
                return false; // We cannot determine if the Reader is ready until we try to get it
            }
            @Override
            public void close() {
                // No resources to close in this implementation, the caller is responsible for closing the Reader and the underlying InputStream
            }
        };
    }

    @Override
    public FutureResponse<LargeObjectCache> getLargeObjectCache(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        return FutureResponse.returns(new LargeObjectCache() {
            @Override
            public Optional<Path> find() {
                return Optional.empty();
            }
            @Override
            public void close() {
                // No resources to close in this implementation
            }
        });
    }

    @Override
    public FutureResponse<Void> copyTo(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref, @Nonnull Path destination) throws BlobException {
        try {
            openBlobRelayStreaming();
            try {
                if (Files.exists(destination)) {
                    throw new BlobException("Destination file already exists: " + destination);
                }
            } catch (IOException e) {
                throw new BlobException("Failed to prepare destination path: " + destination, e);
            }
            try {
                return blobRelayStreaming.get(newGetStreamingRequest(contextId, ref), destination);
            } catch (IOException | InterruptedException e) {
                throw new BlobException("Failed to copy to destination", e);
            }
        } catch (IOException e) {
            throw new BlobException("Failed to copy to destination", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (blobRelayStreaming != null) {
            blobRelayStreaming.close();
        }
    }

    @Override
    public String toString() {
        return "LargeObjectClientRelay{"
             + "sessionId=" + sessionId
             + ", endpoint='" + endpoint + '\''
             + ", secure=" + secure
             + ", chunkSize=" + chunkSize
             + '}';
    }
}
