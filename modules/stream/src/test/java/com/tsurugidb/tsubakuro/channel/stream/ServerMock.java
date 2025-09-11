/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.channel.stream;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.framework.proto.FrameworkResponse;

public class ServerMock {
    static final Logger LOG = LoggerFactory.getLogger(ServerMock.class);

    private final ConcurrentLinkedQueue<ResponseMessage> registeredMessages = new ConcurrentLinkedQueue<>();
    private Worker worker = null;
    private ServerSocket serverSocket = null;

    final class ResponseMessage {
        private final byte[] body;

        ResponseMessage(byte[] responseMessage) {
            this.body = responseMessage;
        }

        byte[] getBytes() {
            return body;
        }
    }

    private class Worker extends Thread {
        private ServerStreamLink serverStreamLink = null;

        Worker() throws IOException {
        }
        @Override
        public void run() {
            try {
                this.serverStreamLink = new ServerStreamLink(serverSocket.accept());
                while (serverStreamLink.receive()) {
                    LOG.debug("received: ", serverStreamLink.getInfo());
                    var slot = serverStreamLink.getSlot();
                    switch (serverStreamLink.getInfo()) {
                        case 2: // StreamLink.REQUEST_SESSION_PAYLOAD
                            var responseMessage = registeredMessages.poll();
                            if (responseMessage != null) {
                                serverStreamLink.sendResponse(slot, responseMessage.getBytes());
                            } else {
                                try {
                                    Thread.sleep(100);
                                } catch(InterruptedException e) {
                                    return;
                                }
                            }
                            break;
                        default:
                            throw new AssertionError("Unexpected message type: " + serverStreamLink.getInfo());
                    }
                }
            } catch (SocketException e) {
                LOG.info("Socket closed");
            } catch (EOFException e) {
                LOG.info("Connection closed by client");
            } catch (IOException e) {
                LOG.error("IOException", e);
            } finally {
                try {
                    close();
                } catch (IOException e) {
                    LOG.error("IOException on close", e);
                }
            }
        }
        void close() throws IOException {
            if (serverStreamLink != null) {
                serverStreamLink.close();
                serverStreamLink = null;
            }
        }
    }

    public ServerMock(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        worker = new Worker();
        worker.start();
    }

    public void close() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
            serverSocket = null;
        }
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            worker = null;
        }
    }

    public boolean next(@Nonnull Message payload) throws IOException {
        return next(toByteArray(payload));
    }

    private byte[] toByteArray(@Nonnull Message payload) {
        var header = FrameworkResponse.Header.newBuilder().setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT).build();
        try (var buffer = new ByteArrayOutputStream()) {
            header.writeDelimitedTo(buffer);
            payload.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    private boolean next(byte [] responseMessage) {
        return registeredMessages.offer(new ResponseMessage(responseMessage));
    }

    public boolean hasRemaining() {
        return !(registeredMessages.isEmpty());
    }
}
