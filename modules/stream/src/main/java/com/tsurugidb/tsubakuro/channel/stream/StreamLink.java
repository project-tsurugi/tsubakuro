/*
 * Copyright 2023-2024 Project Tsurugi.
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

import java.io.DataInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.LinkMessage;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetBox;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetWireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;

public final class StreamLink extends Link {
    private Socket socket;
    private BufferedOutputStream outStream;
    private DataInputStream inStream;
    private ResultSetBox resultSetBox = new ResultSetBox();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean socketError = new AtomicBoolean();
    private final AtomicBoolean socketClosed = new AtomicBoolean();

    public static final int STREAM_HEADER_SIZE = 7;

    // 1 is nolonger used
    private static final byte REQUEST_SESSION_PAYLOAD = 2;
    private static final byte REQUEST_RESULT_SET_BYE_OK = 3;

    public static final byte RESPONSE_SESSION_PAYLOAD = 1;
    public static final byte RESPONSE_RESULT_SET_PAYLOAD = 2;
    // 3, 4 are nolonger used
    public static final byte RESPONSE_RESULT_SET_HELLO = 5;
    public static final byte RESPONSE_RESULT_SET_BYE = 6;
    public static final byte RESPONSE_SESSION_BODYHEAD = 7;

    private static final long SESSION_ID_IS_NOT_ASSIGNED = Long.MAX_VALUE;

    static final Logger LOG = LoggerFactory.getLogger(StreamLink.class);

    public StreamLink(String hostname, int port) throws IOException {
        this.socket = new Socket(hostname, port);
        this.socket.setTcpNoDelay(true);
        this.outStream = new BufferedOutputStream(socket.getOutputStream());
        this.inStream = new DataInputStream(socket.getInputStream());
        super.sessionId = SESSION_ID_IS_NOT_ASSIGNED;
    }

    public void setSessionId(long id) throws IOException {
        if (sessionId == SESSION_ID_IS_NOT_ASSIGNED) {
            this.sessionId = id;
            return;
        }
        throw new IOException("session ID is already assigned");
    }

    @Override
    public boolean doPull(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return doPull(timeout, unit, false);
        } catch (IOException e) {  // IOException is never thrown when throwException is false
            LOG.error("catch exception that never thrown");
            LOG.error(e.getMessage());
        }
        return false;
    }

    private boolean doPull(long timeout, TimeUnit unit, boolean throwException) throws TimeoutException, IOException {
        LinkMessage message = null;
        try {
            int millis = ((timeout == 0) ? 0 : ((unit.toMillis(timeout) > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) unit.toMillis(timeout)));
            socket.setSoTimeout(millis);
        } catch (SocketException e) {
            if (throwException) {
                throw e;
            } else {
                socketError.set(true);
                closeBoxes(false);
                return false;
            }
        }
        try {
            message = receive();
        } catch (SocketTimeoutException e) {
            throw new TimeoutException("response has not been received within the specified time");
        } catch (EOFException e) {   // imply session close
            closeBoxes(true);
            return false;
        } catch (IOException e) {
            if (throwException) {
                throw e;
            } else {
                socketError.set(true);
                closeBoxes(false);
                return false;
            }
        }

        byte info = message.getInfo();
        int slot = message.getSlot();
        switch (info) {

        case RESPONSE_SESSION_PAYLOAD:
            LOG.trace("receive SESSION_PAYLOAD, slot = {}", slot);
            responseBox.push(slot, message.getBytes());
            return true;

        case RESPONSE_SESSION_BODYHEAD:
            LOG.trace("receive RESPONSE_SESSION_BODYHEAD, slot = {}", slot);
            responseBox.pushHead(slot, message.getBytes(), createResultSetWire());
            return true;

        case RESPONSE_RESULT_SET_PAYLOAD:
            byte writer = message.getWriter();
            LOG.trace("receive RESULT_SET_PAYLOAD, slot = {}, writer = {}", slot, writer);
            resultSetBox.push(slot, writer, message.getBytes());
            return true;

        case RESPONSE_RESULT_SET_HELLO:
            LOG.trace("receive RESPONSE_RESULT_SET_HELLO");
            resultSetBox.pushHello(message.getString(), slot);
            return true;

        case RESPONSE_RESULT_SET_BYE:
            LOG.trace("receive RESPONSE_RESULT_SET_BYE");
            try {
                send(REQUEST_RESULT_SET_BYE_OK, slot);
            } catch (IOException e) {
                resultSetBox.pushBye(slot, e);
                return false;
            }
            resultSetBox.pushBye(slot);
            return true;

        default:
            if (throwException) {
                throw new IOException("invalid info in the response");
            } else {
                socketError.set(true);
                closeBoxes(false);
                return false;
            }
        }
    }

    private int closeTimeoutMillis() {
        if (closeTimeout == 0) {
            return 0;
        }
        if (closeTimeUnit.toMillis(closeTimeout) > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
         }
         return (int) closeTimeUnit.toMillis(closeTimeout);
    }

    private void closeBoxes(boolean intentionalClose) throws IOException {
        responseBox.doClose(intentionalClose);
        resultSetBox.doClose(intentionalClose);
        if (!socket.isClosed()) {
            socket.setSoTimeout(closeTimeoutMillis());
            socket.close();
        }
        socketClosed.set(true);
    }

    public ResultSetBox getResultSetBox() {
        return resultSetBox;
    }

    private void send(byte i, int s) throws IOException {  // RESULT_SET_BYE_OK
        byte[] header = new byte[STREAM_HEADER_SIZE];

        header[0] = i;  // info
        header[1] = strip(s);       // slot
        header[2] = strip(s >> 8);  // slot
        header[3] = 0;
        header[4] = 0;
        header[5] = 0;
        header[6] = 0;

        synchronized (outStream) {
            if (socket.isClosed()) {
                throw new IOException("socket is already closed");
            }
            outStream.write(header, 0, header.length);
            outStream.flush();
        }
        LOG.trace("send RESULT_SET_BYE_OK, slot = {}", s); //$NON-NLS-1$
    }

    @Override
    public void send(int s, byte[] frameHeader, byte[] payload, ChannelResponse channelResponse) {  // SESSION_PAYLOAD
        var headerLength = frameHeader.length;
        var payloadLength = payload.length;
        int length = headerLength + payloadLength;
        byte[] whole = new byte[STREAM_HEADER_SIZE + length];

        whole[0] = REQUEST_SESSION_PAYLOAD;
        whole[1] = strip(s);       // slot
        whole[2] = strip(s >> 8);  // slot
        whole[3] = strip(length);
        whole[4] = strip(length >> 8);
        whole[5] = strip(length >> 16);
        whole[6] = strip(length >> 24);
        System.arraycopy(frameHeader, 0, whole, STREAM_HEADER_SIZE, headerLength);
        System.arraycopy(payload, 0, whole, STREAM_HEADER_SIZE +  headerLength, payloadLength);

        synchronized (outStream) {
            if (socket.isClosed()) {
                channelResponse.setMainResponse(new IOException("socket is already closed"));
                return;
            }
            try {
                outStream.write(whole, 0, whole.length);
                outStream.flush();
            } catch (IOException e) {
                channelResponse.setMainResponse(e);
                return;
            }
        }
        LOG.trace("send SESSION_PAYLOAD, length = {}, slot = {}", length, s);
    }

    private byte strip(int i) {
        return (byte) (i & 0xff);
    }

    private LinkMessage receive() throws IOException, SocketTimeoutException {
        synchronized (inStream) {
            try {
                byte[] bytes;
                byte writer = 0;
                byte info = 0;

                // info受信
                info = inStream.readByte();

                // slot受信
                int slot = 0;
                for (int i = 0; i < 2; i++) {
                    int inData = inStream.readByte() & 0xff;
                    slot |= inData << (i * 8);
                }

                if (info ==  RESPONSE_RESULT_SET_PAYLOAD) {
                    writer = inStream.readByte();
                }

                // length受信
                int length = 0;
                for (int i = 0; i < 4; i++) {
                    int inData = inStream.readByte() & 0xff;
                    length |= inData << (i * 8);
                }
                if (length > 0) {
                    // payload受信
                    bytes = new byte[length];
                    inStream.readFully(bytes, 0, length);
                } else {
                    bytes = null;
                }
                return new LinkMessage(info, bytes, slot, writer);
            } catch (SocketException e) {
                socketError.set(true);
                throw e;
            }
        }
    }

    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        return new ResultSetWireImpl(this);
    }

    @Override
    public boolean isAlive() {
        if (closed.get()) {
            return false;
        }
        synchronized (outStream) {
            try {
                socket.sendUrgentData(0);
            } catch (IOException e) {
                return false;
            }
        }
        return !socket.isClosed();  // Practically the same as return true
    }

    @Override
    public String linkLostMessage() {
        return "lost connection";
    }

    @Override
    public void close() throws IOException, ServerException {
        if (!closed.getAndSet(true) && !socketError.get()) {
            try (var c1 = socket; var c2 = inStream; var c3 = outStream) {
                c1.close();
                closeBoxes(true);
            } catch (Exception e) {
                socketError.set(true);
                closeBoxes(false);
                throw e;
            }
        }
    }

    /**
     * Close the socket without sending REQUEST_SESSION_BYE.
     * This method is intended to use before session open.
     * @throws IOException if I/O error was occurred while close the socket
     */
    public void closeWithoutGet() throws IOException {
        closed.set(true);
        closeBoxes(false);
    }
}
