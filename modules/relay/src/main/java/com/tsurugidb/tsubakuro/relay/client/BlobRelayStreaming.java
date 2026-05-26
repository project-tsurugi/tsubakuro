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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.StreamObserver;

import com.tsurugidb.blob_relay.proto.BlobRelayStreamingGrpc;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;

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
        final AtomicBoolean completed = new AtomicBoolean(false);
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();

        final class PutResponseObserver implements StreamObserver<Streaming.PutStreamingResponse> {
            @Override
            public void onNext(Streaming.PutStreamingResponse response) {
                reference.set(response.getBlob());
            }

            @Override
            public void onError(Throwable t) {
                lock.lock();
                try {
                    error.set(t);
                    completed.set(true);
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void onCompleted() {
                lock.lock();
                try {
                    completed.set(true);
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
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

        lock.lock();
        try {
            while (!completed.get()) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        } finally {
            lock.unlock();
        }
        Throwable cause = error.get();
        if (cause != null) {
            close();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw (InterruptedException) cause;
            }
            throw new IOException("Failed to retrieve blob data", cause);
        }
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

    /**
     * Receives Blob data from the gRPC server.
     * @param request the request containing the reference to the Blob data to retrieve
     * @param outputStream the OutputStream to write the retrieved Blob data to
     * @param timeout the timeout in milliseconds for the gRPC call
     * @param timeUnit the TimeUnit for the timeout
     * @throws IOException if an I/O error occurs while writing to the OutputStream
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public void get(Streaming.GetStreamingRequest request, OutputStream outputStream, long timeout, TimeUnit timeUnit) throws IOException, InterruptedException {
        final AtomicReference<Streaming.GetStreamingResponse.Metadata> reference = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicLong totalBytes = new AtomicLong(0);
        final AtomicBoolean completed = new AtomicBoolean(false);
        Lock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
        final class GetResponseObserver implements StreamObserver<Streaming.GetStreamingResponse> {
            @Override
            public void onNext(Streaming.GetStreamingResponse response) {
                switch (response.getPayloadCase()) {
                    case CHUNK:
                        try {
                            var chunk = response.getChunk();
                            totalBytes.addAndGet(chunk.size());
                            bufferedOutputStream.write(chunk.toByteArray());
                            bufferedOutputStream.flush();
                        } catch (IOException e) {
                            // Handle the exception
                        }
                        break;
                    case METADATA:
                        reference.set(response.getMetadata());
                        break;
                    case PAYLOAD_NOT_SET:
                        // Handle the case where no payload is set
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                // Handle the error
                lock.lock();
                try {
                    error.set(t);
                    completed.set(true);
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void onCompleted() {
                // Handle the completion
                try {
                    bufferedOutputStream.close();
                    outputStream.close();
                } catch (IOException e) {
                    // Handle the exception
                }
                lock.lock();
                try {
                    completed.set(true);
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }

        if (timeout > 0 && timeUnit != null) {
            stub.withDeadlineAfter(timeout, timeUnit).get(request, new GetResponseObserver());
        } else {
            stub.get(request, new GetResponseObserver());
        }

        lock.lock();
        try {
            while (!completed.get()) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        } finally {
            lock.unlock();
        }
        Throwable cause = error.get();
        if (cause != null) {
            close();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw (InterruptedException) cause;
            }
            throw new IOException("Failed to retrieve blob data", cause);
        }

        // check the metadata and total bytes
        var metadata = reference.get();
        if (metadata == null) {
            throw new IOException("Failed to retrieve blob data: metadata is missing");
        }
        if (metadata.getBlobSizeOptCase() == Streaming.GetStreamingResponse.Metadata.BlobSizeOptCase.BLOB_SIZE) {
            if (totalBytes.get() != metadata.getBlobSize()) {
                throw new IOException("Failed to retrieve blob data: expected size " + metadata.getBlobSize() + " bytes, but received " + totalBytes.get() + " bytes");
            }
        }
    }

    /**
     * Receives Blob data from the gRPC server.
     * @param request the request containing the reference to the Blob data to retrieve
     * @param outputStream the OutputStream to write the retrieved Blob data to
     * @throws IOException if an I/O error occurs while writing to the OutputStream
     * @throws InterruptedException if the thread is interrupted while waiting for the response
     */
    public void get(Streaming.GetStreamingRequest request, OutputStream outputStream) throws IOException, InterruptedException {
        get(request, outputStream, 0, null);
    }
}