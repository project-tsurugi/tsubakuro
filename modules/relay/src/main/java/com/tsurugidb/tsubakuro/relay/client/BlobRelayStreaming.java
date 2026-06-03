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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * BlobRelayStreaming is a class that provides methods for streaming Blob data using gRPC.
 */
public class BlobRelayStreaming implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BlobRelayStreaming.class);

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
     * @return a reference to the Blob data that was sent
     * @throws IOException if an I/O error occurs while reading the InputStream
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public FutureResponse<BlobRelayCommon.BlobReference> put(Streaming.PutStreamingRequest.Metadata meta, InputStream input) throws IOException, InterruptedException {
        final AtomicReference<BlobRelayCommon.BlobReference> reference = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<CountDownLatch> countDownLatch = new AtomicReference<>(new CountDownLatch(1));

        final StreamObserver<Streaming.PutStreamingRequest> requestObserver = stub.put(new StreamObserver<Streaming.PutStreamingResponse>() {
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
        });

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
            // Mark the end of requests
            requestObserver.onCompleted();
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }

        return new FutureResponse<BlobRelayCommon.BlobReference>() {
            @Override
            public BlobRelayCommon.BlobReference get() throws IOException, InterruptedException, ServerException {
                try {
                    return get(0, null);
                } catch (TimeoutException e) {
                    throw new AssertionError("Unexpected TimeoutException", e);
                }
            }
            @Override
            public BlobRelayCommon.BlobReference get(long timeout, TimeUnit timeUnit) throws IOException, InterruptedException, ServerException, TimeoutException {
                if (timeout < 0) {
                    throw new IllegalArgumentException("timeout must be non-negative: " + timeout);
                }
                CountDownLatch latch = countDownLatch.get();
                if (timeout > 0 && timeUnit != null) {
                    if (!latch.await(timeout, timeUnit)) {
                        throw new TimeoutException("Timeout while waiting for response after " + timeout + " " + timeUnit);
                    }
                } else {
                    latch.await();
                }
                checkException(error.get());
                return reference.get();
            }
            @Override
            public boolean isDone() {
                return countDownLatch.get().getCount() == 0;
            }
            @Override
            public void close() {
                if (!isDone()) {
                    LOG.error("Warning: FutureResponse was closed before completion, cancelling the RPC");
                    requestObserver.onError(new IOException("FutureResponse was closed before completion"));
                }
            }
        };
    }

    private static class CustomPipedInputStream extends PipedInputStream {
        private final AtomicReference<Throwable> exceptionRef = new AtomicReference<>(null);
        private final AtomicBoolean pipedOutputStreamIsClosed = new AtomicBoolean(false);
        private final PipedOutputStream pipedOutputStream;
        long timeout = 0;
        TimeUnit timeUnit = null;

        CustomPipedInputStream(PipedOutputStream pipedOutputStream, int pipeSize) throws IOException {
            super(pipedOutputStream, pipeSize);
            this.pipedOutputStream = pipedOutputStream;
        }

        public void setException(Throwable e) {
            exceptionRef.compareAndSet(null, e);
        }

        @Override
        public int read() throws IOException {
            waitforData();
            return super.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            waitforData();
            return super.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            waitforData();
            return super.read(b, off, len);
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

        synchronized void setTimeout(long t, TimeUnit u) {
            timeout = t;
            timeUnit = u;
        }

        private void waitforData() throws IOException {
            synchronized (this) {
                while (available() < 1 && !pipedOutputStreamIsClosed.get()) {
                    try {
                        if (timeout > 0 && timeUnit != null) {
                            long timeoutMillis = timeUnit.toMillis(timeout);
                            long startTime = System.currentTimeMillis();
                            wait(timeoutMillis);
                            long elapsed = System.currentTimeMillis() - startTime;
                            if (elapsed >= timeoutMillis) {
                                throw new ResponseTimeoutException("Timeout while waiting for data after " + timeout + " " + timeUnit);
                            }
                        } else {
                            wait();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Thread was interrupted while waiting for data", e);
                    }
                }
                checkException();
            }
        }

        void notifyDataAvailable(boolean closed) {
            synchronized (this) {
                pipedOutputStreamIsClosed.set(closed);
                notifyAll();
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
     * @return an InputStream to read the Blob data
     * @throws IOException if an I/O error occurs while reading the BLOB data / while writing to the file at path
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public FutureResponse<InputStream> get(Streaming.GetStreamingRequest request) throws IOException, InterruptedException {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final PipedOutputStream pipedOutputStream = new PipedOutputStream();
        final CustomPipedInputStream pipedInputStream = new CustomPipedInputStream(pipedOutputStream, (int) chunkSize);

        stub.get(request, new StreamObserver<Streaming.GetStreamingResponse>() {
            Streaming.GetStreamingResponse.Metadata metadata = null;
            long totalBytes = 0;

            @Override
            public void onNext(Streaming.GetStreamingResponse response) {
                switch (response.getPayloadCase()) {
                    case CHUNK:
                        try {
                            var chunk = response.getChunk();
                            totalBytes += chunk.size();
                            var bytes = chunk.toByteArray();
                            int offset = 0;
                            while (offset < bytes.length) {
                                int length = (int) Math.min(chunkSize, bytes.length - offset);
                                pipedOutputStream.write(bytes, offset, length);
                                pipedOutputStream.flush();
                                pipedInputStream.notifyDataAvailable(false);
                                offset += length;
                            }
                        } catch (IOException e) {
                            // Handle the exception
                            LOG.error("Error while writing chunk data to pipedOutputStream: {}", e.getMessage(), e);
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
            private synchronized void setError(Throwable e) {
                var err = error.get();
                if (err != null) {
                    err.addSuppressed(e);
                } else {
                    err = e;
                }
                pipedInputStream.setException(err);
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
                        pipedOutputStream.close();
                        pipedInputStream.notifyDataAvailable(true);
                    } catch (IOException e) {
                        // Handle the exception
                        setError(e);
                    }
                }
            }

            @Override
            public void onCompleted() {
                // Handle the completion
                try {
                    pipedOutputStream.close();
                    pipedInputStream.notifyDataAvailable(true);
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
        });

        return new FutureResponse<InputStream>() {
            @Override
            public InputStream get() {
                return get(0, null);
            }
            @Override
            public InputStream get(long timeout, TimeUnit timeUnit) {
                pipedInputStream.setTimeout(timeout, timeUnit);
                return pipedInputStream;
            }
            @Override
            public boolean isDone() {
                return true;
            }
            @Override
            public void close() throws IOException {
                // do nothing, as the pipedInputStream will be closed by the onCompleted or onError callback
            }
        };
    }

    /**
     * Receives Blob data from the gRPC server.
     * @param request the request containing the reference to the Blob data to retrieve
     * @param destination the Path to write the Blob data to
     * @return a FutureResponse that completes when the Blob data has been fully received and written to the OutputStream
     * @throws IOException if an I/O error occurs while reading the BLOB data / while writing to the OutputStream
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public FutureResponse<Void> get(@Nonnull Streaming.GetStreamingRequest request, @Nonnull Path destination) throws IOException, InterruptedException {
        final OutputStream outputStream = Files.newOutputStream(destination);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<CountDownLatch> countDownLatch = new AtomicReference<>(new CountDownLatch(1));

        final class GetResponseObserver implements StreamObserver<Streaming.GetStreamingResponse> {
            Streaming.GetStreamingResponse.Metadata metadata = null;
            long totalBytes = 0;

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
            private synchronized void setError(Throwable e) {
                var err = error.get();
                if (err != null) {
                    err.addSuppressed(e);
                } else {
                    err = e;
                }
                error.set(err);
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
                setError(isDeadlineExceeded ? new ResponseTimeoutException(t) : t);
            }

            @Override
            public void onCompleted() {
                // Handle the completion
                if (countDownLatch.get().getCount() > 0) {
                    countDownLatch.get().countDown();
                }
                try {
                    outputStream.close();
                } catch (IOException e) {
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
        final GetResponseObserver responseObserver = new GetResponseObserver();
        stub.get(request, responseObserver);

        return new FutureResponse<Void>() {
            @Override
            public Void get() throws IOException, InterruptedException, ServerException {
                try {
                    return get(0, null);
                } catch (TimeoutException e) {
                    throw new AssertionError("Unexpected TimeoutException", e);
                }
            }
            @Override
            public Void get(long timeout, TimeUnit timeUnit) throws IOException, InterruptedException, ServerException, TimeoutException {
                if (timeout < 0) {
                    throw new IllegalArgumentException("timeout must be non-negative: " + timeout);
                }
                CountDownLatch latch = countDownLatch.get();
                if (timeout > 0 && timeUnit != null) {
                    if (!latch.await(timeout, timeUnit)) {
                        responseObserver.setError(new ResponseTimeoutException("Timeout while waiting for response after " + timeout + " " + timeUnit));
                        throw new TimeoutException("Timeout while waiting for response after " + timeout + " " + timeUnit);
                    }
                } else {
                    latch.await();
                }
                checkException(error.get());
                return null;
            }
            @Override
            public boolean isDone() {
                return countDownLatch.get().getCount() == 0;
            }
            @Override
            public void close() throws IOException {
                if (error.get() != null) {
                    Files.deleteIfExists(destination);
                }
                if (!isDone()) {
                    LOG.error("Warning: FutureResponse was closed before completion, cancelling the RPC");
                    responseObserver.onCompleted();
                }
            }
        };
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