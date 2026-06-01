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
package com.tsurugidb.tsubakuro.relay.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.tsurugidb.blob_relay.proto.BlobRelayStreamingGrpc;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;

/**
 * BlobRelayStreaming is a class that provides methods for streaming Blob data using gRPC.
 */
public class BlobRelayStreaming implements Closeable {
    private static final long CLOSE_TIMEOUT = 1_000; // 1 second
    private final BlobRelayStreamingGrpc.BlobRelayStreamingStub stub;
    private final long chunkSize;
    private final ManagedChannel channel;

    /**
     * Creates a new BlobRelayStreaming.
     * @param endpoint the gRPC server endpoint to connect to
     * @param secure whether to use secure connection for the BlobRelayStreaming
     * @param chunkSize the size of each chunk to be sent
     */
    public BlobRelayStreaming(@Nonnull String endpoint, boolean secure, long chunkSize) {
        this.chunkSize = validateChunkSize(chunkSize);
        this.channel = Grpc.newChannelBuilder(endpoint, secure ? TlsChannelCredentials.create() : InsecureChannelCredentials.create()).build();
        this.stub = BlobRelayStreamingGrpc.newStub(channel);
    }
    private static long validateChunkSize(long chunkSize) {
         if (chunkSize <= 0 || chunkSize > Integer.MAX_VALUE) {
             throw new IllegalArgumentException("chunkSize must be between 1 and " + Integer.MAX_VALUE + ": " + chunkSize);
         }
         return chunkSize;
     }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(CLOSE_TIMEOUT, TimeUnit.MILLISECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    /**
     * Sends Blob data to the gRPC server.
     * @param meta the metadata associated with the Blob data to send
     * @param input the InputStream containing the Blob data to send
     * @param timeout the timeout in milliseconds for the gRPC call
     * @param timeUnit the TimeUnit for the timeout
     * @return a reference to the Blob data that was sent
     * @throws IOException if an I/O error occurs while reading the InputStream
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public BlobRelayCommon.BlobReference put(Streaming.PutStreamingRequest.Metadata meta, InputStream input, long timeout, TimeUnit timeUnit) throws IOException, InterruptedException {
        final AtomicReference<BlobRelayCommon.BlobReference> reference = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<CountDownLatch> countDownLatch = new AtomicReference<>(new CountDownLatch(1));

        final class PutResponseObserver implements StreamObserver<Streaming.PutStreamingResponse> {
            @Override
            public void onNext(Streaming.PutStreamingResponse response) {
                reference.set(response.getBlob());
            }

            @Override
            public void onError(Throwable t) {
                // Handle the error
                boolean isDeadlineExceeded = false;
                if (t instanceof StatusRuntimeException) {
                    StatusRuntimeException statusEx = (StatusRuntimeException) t;
                    if (statusEx.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                        isDeadlineExceeded = true;
                    }
                }
                error.set(isDeadlineExceeded ? new ResponseTimeoutException(t) : t);
                countDownLatch.get().countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.get().countDown();
            }
        }

        StreamObserver<Streaming.PutStreamingRequest> requestObserver = (timeout > 0 && timeUnit != null)
            ? stub.withDeadlineAfter(timeout, timeUnit).put(new PutResponseObserver())
            : stub.put(new PutResponseObserver());

        try {
            requestObserver.onNext(Streaming.PutStreamingRequest.newBuilder()
                                    .setMetadata(meta)
                                    .build());

            byte[] buffer = new byte[(int) chunkSize];
            int bytesRead;
            while ((bytesRead = input.read(buffer)) != -1) {
                requestObserver.onNext(Streaming.PutStreamingRequest.newBuilder()
                                        .setChunk(com.google.protobuf.ByteString.copyFrom(buffer, 0, bytesRead))
                                        .build());
            }
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        countDownLatch.get().await();
        checkException(error.get());
        return reference.get();
    }

    /**
     * Sends Blob data to the gRPC server.
     * @param meta the metadata associated with the Blob data to send
     * @param input the InputStream containing the Blob data to send
     * @return a reference to the Blob data that was sent
     * @throws IOException if an I/O error occurs while reading the InputStream
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public BlobRelayCommon.BlobReference put(Streaming.PutStreamingRequest.Metadata meta, InputStream input) throws IOException, InterruptedException {
        return put(meta, input, 0, null);
    }

    private static class CustomPipedInputStream extends PipedInputStream {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>(null);
        private final PipedOutputStream pipedOutputStream;

        CustomPipedInputStream(PipedOutputStream pipedOutputStream, int pipeSize) throws IOException {
            super(pipedOutputStream, pipeSize);
            this.pipedOutputStream = pipedOutputStream;
        }

        public void setException(Throwable e) {
            exceptionRef.compareAndSet(null, e);
        }

        @Override
        public int read() throws IOException {
            int rv = super.read();
            checkException();
            return rv;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int rv = super.read(b);
            checkException();
            return rv;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int rv = super.read(b, off, len);
            checkException();
            return rv;
        }

        @Override
        public void close() throws IOException {
            try {
                checkException();
            } finally {
                super.close();
                pipedOutputStream.close();
            }
        }

        private void checkException() throws IOException {
            Throwable e = exceptionRef.get();
            if (e != null) {
                if (e instanceof IOException) {
                    throw (IOException) e;
                } else if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Thread was interrupted", e);
                }
                throw new IOException("Unexpected exception occurred", e);
            }
        }
    }

    /**
     * Receives Blob data from the gRPC server.
     * @param request the request containing the reference to the Blob data to retrieve
     * @param timeout the timeout in milliseconds for the gRPC call
     * @param timeUnit the TimeUnit for the timeout
     * @return an InputStream to read the Blob data
     * @throws IOException if an I/O error occurs while reading the BLOB data / while writing to the file at path
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public InputStream get(Streaming.GetStreamingRequest request, long timeout, TimeUnit timeUnit) throws IOException, InterruptedException {
        return getInternal(request, null, timeout, timeUnit);
    }

    /**
     * Receives Blob data from the gRPC server.
     * @param request the request containing the reference to the Blob data to retrieve
     * @return an InputStream to read the Blob data
     * @throws IOException if an I/O error occurs while reading the BLOB data / while writing to the file at path
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public InputStream get(Streaming.GetStreamingRequest request) throws IOException, InterruptedException {
        return get(request, 0, null);
    }

    /**
     * Receives Blob data from the gRPC server.
     * @param request the request containing the reference to the Blob data to retrieve
     * @param path the Path to write the Blob data to
     * @param timeout the timeout in milliseconds for the gRPC call
     * @param timeUnit the TimeUnit for the timeout
     * @throws IOException if an I/O error occurs while reading the BLOB data / while writing to the file at path
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public void get(@Nonnull Streaming.GetStreamingRequest request, @Nonnull Path path, long timeout, TimeUnit timeUnit) throws IOException, InterruptedException {
        getInternal(request, path, timeout, timeUnit);
    }

    /**
     * Receives Blob data from the gRPC server.
     * @param request the request containing the reference to the Blob data to retrieve
     * @param path the Path to write the Blob data to
     * @throws IOException if an I/O error occurs while reading the BLOB data / while writing to the file at path
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public void get(@Nonnull Streaming.GetStreamingRequest request, @Nonnull Path path) throws IOException, InterruptedException {
        get(request, path, 0, null);
    }

    private InputStream getInternal(@Nonnull Streaming.GetStreamingRequest request, @Nullable Path path, long timeout, @Nullable TimeUnit timeUnit) throws IOException, InterruptedException {
        final boolean writeToFile = (path != null);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<CustomPipedInputStream> inputStream = new AtomicReference<>(null);
        final AtomicReference<CountDownLatch> countDownLatch = new AtomicReference<>(writeToFile ? new CountDownLatch(1) : null);

        final class GetResponseObserver implements StreamObserver<Streaming.GetStreamingResponse> {
            final OutputStream outputStream;

            Streaming.GetStreamingResponse.Metadata metadata = null;
            long totalBytes = 0;

            GetResponseObserver(OutputStream outputStream) {
                this.outputStream = outputStream;
            }

            @Override
            public void onNext(Streaming.GetStreamingResponse response) {
                switch (response.getPayloadCase()) {
                    case CHUNK:
                        try {
                            var chunk = response.getChunk();
                            totalBytes += chunk.size();
                            outputStream.write(chunk.toByteArray());
                            outputStream.flush();
                        } catch (IOException e) {
                            // Handle the exception
                            setError(e);
                        }
                        break;
                    case METADATA:
                        metadata = response.getMetadata();
                        break;
                    case PAYLOAD_NOT_SET:
                        // Handle the case where no payload is set
                        break;
                }
            }
            synchronized private void setError(Throwable e) {
                var err = error.get();
                if (err != null) {
                    err.addSuppressed(e);
                } else {
                    err = e;
                }
                if (writeToFile) {
                    error.set(err);
                } else {
                    if (inputStream.get() != null) {
                        inputStream.get().setException(err);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                // Handle the error
                try {
                    boolean isDeadlineExceeded = false;
                    if (t instanceof StatusRuntimeException) {
                        StatusRuntimeException statusEx = (StatusRuntimeException) t;
                        if (statusEx.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                            isDeadlineExceeded = true;
                        }
                    }
                    setError(isDeadlineExceeded ? new ResponseTimeoutException(t) : t);
                } finally {
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        // Handle the exception
                        setError(e);
                    }
                    if (writeToFile) {
                        countDownLatch.get().countDown();
                    }
                }
            }

            @Override
            public void onCompleted() {
                if (writeToFile) {
                    countDownLatch.get().countDown();
                }
                // Handle the completion
                try {
                    outputStream.close();
                } catch (IOException e) {
                    // Handle the exception
                    setError(e);
                }
                // check the metadata and total bytes
                if (metadata == null) {
                    setError(new IOException("Failed to retrieve blob data, as metadata is missing"));
                } else {
                    if (metadata.getBlobSizeOptCase() == Streaming.GetStreamingResponse.Metadata.BlobSizeOptCase.BLOB_SIZE) {
                        if (totalBytes != metadata.getBlobSize()) {
                            setError(new IOException("Failed to retrieve blob data, as expected size " + metadata.getBlobSize() + " bytes does not match received size " + totalBytes + " bytes"));
                        }
                    }
                }
            }
        }

        GetResponseObserver responseObserver;
        if (writeToFile) {
            responseObserver = new GetResponseObserver(new FileOutputStream(path.toFile()));
        } else {
            PipedOutputStream pipedOutputStream = new PipedOutputStream();
            responseObserver = new GetResponseObserver(pipedOutputStream);
            inputStream.set(new CustomPipedInputStream(pipedOutputStream, (int) chunkSize));
        }

        if (timeout > 0 && timeUnit != null) {
            stub.withDeadlineAfter(timeout, timeUnit).get(request, responseObserver);
        } else {
            stub.get(request, responseObserver);
        }

        if (writeToFile) {
            countDownLatch.get().await();
            checkException(error.get());
            return null;
        } else {
            return inputStream.get();
        }
    }
    private static void checkException(Throwable e) throws IOException {
        if (e != null) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new IOException("Thread was interrupted", e);
            }
            throw new IOException("Unexpected exception occurred", e);
        }
    }
}