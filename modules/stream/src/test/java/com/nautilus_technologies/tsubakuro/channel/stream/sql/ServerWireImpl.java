package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.ServerSocket;
// import java.text.MessageFormat;
// import java.util.Objects;
import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.stream.ServerStreamWire;
import com.tsurugidb.tateyama.proto.SqlRequest;
import com.tsurugidb.tateyama.proto.SqlResponse;
import com.tsurugidb.tateyama.proto.FrameworkRequest;
import com.tsurugidb.tateyama.proto.FrameworkResponse;

/**
 * ServerWireImpl type.
 */
public class ServerWireImpl implements Closeable {
    static final FrameworkResponse.Header.Builder HEADER_BUILDER = FrameworkResponse.Header.newBuilder();

    static final Logger LOG = LoggerFactory.getLogger(ServerWireImpl.class);

    private ServerStreamWire serverStreamWire;
    private final ArrayDeque<Message> receiveQueue;
    private final ReceiveWorker receiver;
    private final ArrayDeque<Message> sendQueue;
    private SendWorker sender;
    private final long sessionID;

    private static class Message {
        public byte[] bytes;

        Message(byte[] bytes) {
            this.bytes = bytes;
        }
        byte[] getBytes() {
            return bytes;
        }
    }

    private class ReceiveWorker extends Thread {
        private final int port;
        ServerSocket serverSocket;

        ReceiveWorker(int port) {
            this.port = port;
        }
        @Override
        public void run() {
            LOG.info("start listen TCP/IP port: {}", port);
            try {
                serverSocket = new ServerSocket(port);

                serverStreamWire = new ServerStreamWire(serverSocket.accept());
                LOG.info("accept client: {}", port);
                while (serverStreamWire.receive()) {
                    LOG.debug("received: ", serverStreamWire.getInfo());
                    if (serverStreamWire.getInfo() != 3) {  // StreamWire.REQUEST_RESULT_SET_BYE_OK = 3
                        receiveQueue.add(new Message(serverStreamWire.getBytes()));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(e);
            }
        }
        void close() throws IOException {
            serverSocket.close();
        }
    }

    private class SendWorker extends Thread {
        String name;

        SendWorker(String name) {
            this.name = name;
        }
        @Override
        public void run() {
            try {
                serverStreamWire.sendRecordHello(0, name);
                while (true) {
                    if (serverStreamWire.isSnedOk()) {
                        while (!sendQueue.isEmpty()) {
                            var entry = sendQueue.poll().getBytes();
                            serverStreamWire.sendRecord(0, 0, entry);
                            if (entry.length == 0) {
                                return;
                            }
                        }
                    }
                    waitReady();
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(e);
            }
        }
        synchronized void waitReady() {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println(e);
            }
        }
        synchronized void notifyEvent() {
            notify();
        }
    }

    public ServerWireImpl(int port, long sessionID) throws IOException {
        this.sessionID = sessionID;
        this.receiveQueue = new ArrayDeque<Message>();
        this.sendQueue = new ArrayDeque<Message>();
        this.receiver = new ReceiveWorker(port);
        receiver.start();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.err.println(e);
        }
    }

    @Override
    public void close() throws IOException {
        serverStreamWire.close();
        receiver.close();
    }

    public long getSessionID() {
        return sessionID;
    }

    /**
     * Get SqlRequest.Request from a client via the native wire.
     @returns SqlRequest.Request
     */
    public SqlRequest.Request get() throws IOException {
        try {
            while (true) {
                if (!receiveQueue.isEmpty()) {
                    var ba = receiveQueue.poll().getBytes();
                    var byteArrayInputStream = new ByteArrayInputStream(ba);
                    FrameworkRequest.Header.parseDelimitedFrom(byteArrayInputStream);
                    return SqlRequest.Request.parseDelimitedFrom(byteArrayInputStream);
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.err.println(e);
                }
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: ServerWireImpl.get()");
        }
    }

    /**
     * Put SqlResponse.Response to the client via the native wire.
     @param request the SqlResponse.Response message
     */
    public void put(SqlResponse.Response response) throws IOException {
        var header = HEADER_BUILDER.build();
        try (var buffer = new ByteArrayOutputStream()) {
            header.writeDelimitedTo(buffer);
            response.writeDelimitedTo(buffer);
            var bytes = buffer.toByteArray();
            serverStreamWire.sendResponse(0, bytes);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public long createRSL(String name) throws IOException {
        sender = new SendWorker(name);
        sender.start();
        return 0;
    }

    public void putRecordsRSL(long handle, byte[] ba) throws IOException {
        //    serverStreamWire.sendRecord(0, 0, ba);
        sendQueue.add(new Message(ba));
        sender.notifyEvent();
    }

    public void eorRSL(long handle) throws IOException {
        byte[] ba = new byte[0];
        //    serverStreamWire.sendRecord(0, 0, ba);
        sendQueue.add(new Message(ba));
        sender.notifyEvent();
    }
}
