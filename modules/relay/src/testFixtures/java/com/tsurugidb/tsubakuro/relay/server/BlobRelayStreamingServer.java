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
package com.tsurugidb.tsubakuro.relay.server;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import com.tsurugidb.blob_relay.proto.BlobRelayStreamingGrpc;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;
import com.tsurugidb.blob_relay.proto.Streaming;

/**
 * Server that manages startup/shutdown of a {@code BlobRelay} server.
 */
public class BlobRelayStreamingServer {
    private static final Logger logger = Logger.getLogger(BlobRelayStreamingServer.class.getName());

    private final BlobRelayImpl blobRelayImpl = new BlobRelayImpl();
    private Server server;
    private int port;
    private FaultType injectedFault;

    public void start() throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
            .executor(executor)
            .addService(blobRelayImpl)
            .build()
            .start();
        port = server.getPort();
        logger.info("Server started, listening on " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BlobRelayStreamingServer.this.stop();
                } catch (InterruptedException e) {
                    if (server != null) {
                        server.shutdownNow();
                    }
                    e.printStackTrace(System.err);
                } finally {
                    executor.shutdown();
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public int getPort() {
        return port;
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public enum FaultType {
        NoFault,
        NoResponse,
    }

    public void injectFault(FaultType faultType) {
        this.injectedFault = faultType;
    }

    class BlobRelayImpl extends BlobRelayStreamingGrpc.BlobRelayStreamingImplBase {
        private final ConcurrentLinkedQueue<Streaming.PutStreamingResponse> putResponses = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Streaming.GetStreamingResponse> getResponses = new ConcurrentLinkedQueue<>();
        private final AtomicLong receivedSize = new AtomicLong(0);
        private final AtomicBoolean receivedSizeValid = new AtomicBoolean(false);
        private final AtomicReference<byte[]> receivedData = new AtomicReference<>();
        private final ArrayList<byte[]> receivedDataList = new ArrayList<>();
        private final AtomicInteger receivedDataCount = new AtomicInteger(0);
        private BlobRelayCommon.BlobReference blobReference;

        @Override
        public StreamObserver<Streaming.PutStreamingRequest> put(StreamObserver<Streaming.PutStreamingResponse> responseObserver) {
            if (injectedFault == FaultType.NoResponse) {
                return new StreamObserver<Streaming.PutStreamingRequest>() {
                    @Override
                    public void onNext(Streaming.PutStreamingRequest request) {
                        // Do nothing
                    }

                    @Override
                    public void onError(Throwable t) {
                        // Do nothing
                    }

                    @Override
                    public void onCompleted() {
                        // Do nothing
                    }
                };
            }
            return new StreamObserver<Streaming.PutStreamingRequest>() {
                @Override
                public void onNext(Streaming.PutStreamingRequest request) {
                    switch (request.getPayloadCase()) {
                        case METADATA:
                            if (request.getMetadata().getBlobSizeOptCase() == Streaming.PutStreamingRequest.Metadata.BlobSizeOptCase.BLOB_SIZE) {
                                receivedSize.set(request.getMetadata().getBlobSize());
                                receivedSizeValid.set(true);
                            }
                            break;
                        case CHUNK:
                            var chunk = request.getChunk();
                            if (chunk != null) {
                                var data = chunk.toByteArray();

                                var currentData = receivedData.get();
                                if (currentData == null) {
                                    receivedData.set(data);
                                } else {
                                    byte[] result = new byte[currentData.length + data.length];
                                    System.arraycopy(currentData, 0, result, 0, currentData.length);
                                    System.arraycopy(data, 0, result, currentData.length, data.length);
                                    receivedData.set(result);
                                }
                            }
                            break;
                        default:
                             throw new RuntimeException("Invalid request type: " + request.getPayloadCase());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    // Handle error
                }

                @Override
                public void onCompleted() {
                    if (receivedSizeValid.get()) {
                        if (receivedData.get().length != receivedSize.get()) {
                            throw new RuntimeException("Received data size does not match the expected size: " + receivedSize.get() + " != " + receivedData.get().length);
                        }
                    }
                    receivedDataList.add(receivedData.get());
                    receivedDataCount.incrementAndGet();
                    var response = putResponses.poll();
                    responseObserver.onNext(response);
                    receivedData.set(null);
                    responseObserver.onCompleted();
                }
            };
        }
        byte[] receivedData(int offset) {
            var data = receivedDataList.get(receivedDataCount.get() - 1 + offset);
            if (data == null) {
                return new byte[0];
            }
            return data;
        }

        @Override
        public void get(Streaming.GetStreamingRequest request, StreamObserver<Streaming.GetStreamingResponse> responseObserver) {
            if (injectedFault == FaultType.NoResponse) {
                return;
            }
            blobReference = request.getBlob();
            while (!getResponses.isEmpty()) {
                var response = getResponses.poll();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        }

        // register responses for testing
        void addPutResponse(Streaming.PutStreamingResponse response) {
            putResponses.offer(response);
        }
        void addGetResponse(Streaming.GetStreamingResponse response) {
            getResponses.offer(response);
        }
        BlobRelayCommon.BlobReference getBlobReference() {
            return blobReference;
        }
        boolean hasRemaining() {
            return !(putResponses.isEmpty() && getResponses.isEmpty());
        }
    }

    // register responses for testing
    public void addPutResponse(Streaming.PutStreamingResponse response) {
        blobRelayImpl.addPutResponse(response);
    }
    public void addGetResponse(Streaming.GetStreamingResponse response) {
        blobRelayImpl.addGetResponse(response);
    }

    // for result verification
    public byte[] receivedData() {
        return blobRelayImpl.receivedData(0);
    }
    public byte[] receivedData(int offset) {
        return blobRelayImpl.receivedData(offset);
    }
    public BlobRelayCommon.BlobReference getBlobReference() {
        return blobRelayImpl.getBlobReference();
    }
    public boolean hasRemaining() {
        return blobRelayImpl.hasRemaining();
    }
}