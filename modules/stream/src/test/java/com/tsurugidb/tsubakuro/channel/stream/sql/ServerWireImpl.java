package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
// import java.text.MessageFormat;
import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.stream.ServerStreamLink;

/**
 * ServerWireImpl type.
 */
public class ServerWireImpl implements Closeable {
    static final FrameworkResponse.Header.Builder HEADER_BUILDER = FrameworkResponse.Header.newBuilder();
    static final int SERVICE_ID_ENDPOINT_BROKER = 1;
    static final int SERVICE_ID_SQL = 3;

    static final Logger LOG = LoggerFactory.getLogger(ServerWireImpl.class);

    private ServerStreamLink serverStreamLink;
    private final ArrayDeque<Message> receiveQueue;
    private final ReceiveWorker receiver;
    private final ArrayDeque<Message> sendQueue;
    private SendWorker sender;
    private final long sessionID;
    private int slot;

    private static class Message {
        byte[] bytes;
        Message(byte[] bytes) {
            this.bytes = bytes;
        }
        byte[] getBytes() {
            return bytes;
        }
    }

    private class ReceiveWorker extends Thread {
        private final int port;
        private final boolean doHandshake;
        ServerSocket serverSocket;

        ReceiveWorker(int port, boolean doHandshake) throws IOException {
            this.port = port;
            this.doHandshake = doHandshake;
            serverSocket = new ServerSocket(port);
        }
        @Override
        public void run() {
            try {
                LOG.info("start listen TCP/IP port: {}", port);
                serverStreamLink = new ServerStreamLink(serverSocket.accept());
                LOG.info("accept client: {}", port);
                if (doHandshake) {
                    if (!handshake()) {
                        System.err.println("error in handshake");
                        return;
                    }
                }
                while (serverStreamLink.receive()) {
                    LOG.debug("received: ", serverStreamLink.getInfo());
                    slot = serverStreamLink.getSlot();
                    switch (serverStreamLink.getInfo()) {
                        case 2: // StreamLink.REQUEST_SESSION_PAYLOAD
                            receiveQueue.add(new Message(serverStreamLink.getBytes()));
                            break;
                        case 3: // StreamLink.REQUEST_RESULT_SET_BYE_OK = 3
                            break;
                        case 4: // StreamLink.REQUEST_SESSION_BYE = 4
                            serverStreamLink.close();
                            break;
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
        boolean handshake() {
            try {
                serverStreamLink.receive();
                LOG.debug("received: ", serverStreamLink.getInfo());
                slot = serverStreamLink.getSlot();
                switch (serverStreamLink.getInfo()) {
                    case 2: // StreamLink.REQUEST_SESSION_PAYLOAD
                        break;
                    default:
                        return false;
                }
                var ba = serverStreamLink.getBytes();
                var byteArrayInputStream = new ByteArrayInputStream(ba);
                var reqHeader = FrameworkRequest.Header.parseDelimitedFrom(byteArrayInputStream);
                var request = EndpointRequest.Request.parseDelimitedFrom(byteArrayInputStream);

                var resHeader = FrameworkResponse.Header.newBuilder().build();
                var response = EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder().setSessionId(1))  // who assign this sessionId?
                    .build();

                var buffer = new ByteArrayOutputStream();
                resHeader.writeDelimitedTo(buffer);
                response.writeDelimitedTo(buffer);
                var bytes = buffer.toByteArray();
                serverStreamLink.sendResponse(slot, bytes);
                return true;
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(e);
                return false;
            }
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
                serverStreamLink.sendRecordHello(slot, name);
                while (true) {
                    if (serverStreamLink.isSnedOk()) {
                        while (!sendQueue.isEmpty()) {
                            var entry = sendQueue.poll().getBytes();
                            serverStreamLink.sendRecord(slot, 0, entry);
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

    public ServerWireImpl(int port, long sessionID, boolean doHandshake) throws IOException {
        this.sessionID = sessionID;
        this.receiveQueue = new ArrayDeque<Message>();
        this.sendQueue = new ArrayDeque<Message>();
        this.receiver = new ReceiveWorker(port, doHandshake);
        this.slot = 0;
        receiver.start();
    }

    public ServerWireImpl(int port, long sessionID) throws IOException {
        this(port, sessionID, false);
    }

    @Override
    public void close() throws IOException {
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
                    var reqHeader = FrameworkRequest.Header.parseDelimitedFrom(byteArrayInputStream);
                    if (reqHeader.getServiceId() != SERVICE_ID_SQL) {
                        throw new IOException("error: request error");
                    }
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
            serverStreamLink.sendResponse(slot, bytes);
        }
    }

    public long createRSL(String name) throws IOException {
        sender = new SendWorker(name);
        sender.start();
        return 0;
    }

    public void putRecordsRSL(long handle, byte[] ba) throws IOException {
        //    serverStreamLink.sendRecord(0, 0, ba);
        sendQueue.add(new Message(ba));
        sender.notifyEvent();
    }

    public void eorRSL(long handle) throws IOException {
        byte[] ba = new byte[0];
        //    serverStreamLink.sendRecord(0, 0, ba);
        sendQueue.add(new Message(ba));
        sender.notifyEvent();
    }
}
