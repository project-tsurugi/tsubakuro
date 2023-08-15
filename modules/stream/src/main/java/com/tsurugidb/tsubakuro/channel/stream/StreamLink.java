package com.tsurugidb.tsubakuro.channel.stream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.LinkMessage;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetBox;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetWireImpl;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;

public final class StreamLink extends Link {
    private Socket socket;
    private DataOutputStream outStream;
    private DataInputStream inStream;
    private ResultSetBox resultSetBox = new ResultSetBox();
    private final Lock lock = new ReentrantLock();
    private final AtomicReference<LinkMessage> helloResponse = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean socketError = new AtomicBoolean();
    private final AtomicBoolean socketClosed = new AtomicBoolean();

    private static final byte REQUEST_SESSION_HELLO = 1;
    private static final byte REQUEST_SESSION_PAYLOAD = 2;
    private static final byte REQUEST_RESULT_SET_BYE_OK = 3;
    private static final byte REQUEST_SESSION_BYE = 4;

    public static final byte RESPONSE_SESSION_PAYLOAD = 1;
    public static final byte RESPONSE_RESULT_SET_PAYLOAD = 2;
    public static final byte RESPONSE_SESSION_HELLO_OK = 3;
    public static final byte RESPONSE_SESSION_HELLO_NG = 4;
    public static final byte RESPONSE_RESULT_SET_HELLO = 5;
    public static final byte RESPONSE_RESULT_SET_BYE = 6;
    public static final byte RESPONSE_SESSION_BODYHEAD = 7;
    public static final byte RESPONSE_SESSION_BYE_OK = 8;

    static final Logger LOG = LoggerFactory.getLogger(StreamLink.class);

    public StreamLink(String hostname, int port) throws IOException {
        this.socket = new Socket(hostname, port);
        this.outStream = new DataOutputStream(socket.getOutputStream());
        this.inStream = new DataInputStream(socket.getInputStream());
        this.helloResponse.set(null);
        send(REQUEST_SESSION_HELLO, ResponseBox.responseBoxSize());
    }

    public LinkMessage helloResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        lock.lock();
        try {
            while (helloResponse.get() == null) {
                try {
                    doPull(timeout, unit, true);
                } catch (IOException e) {
                    throw new IOException("Server crashed");
                }
            }
            return helloResponse.get();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean doPull(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return doPull(timeout, unit, false);
        } catch (IOException e) {  // IOException is never thrown when throwException is false
            System.err.println("catch exception that never thrown");
            System.err.println(e);
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

        case RESPONSE_SESSION_HELLO_OK:
        case RESPONSE_SESSION_HELLO_NG:
            LOG.trace("receive SESSION_HELLO_{}", ((info == RESPONSE_SESSION_HELLO_OK) ? "OK" : "NG"));
            lock.lock();
            try {
                helloResponse.set(message);
            } finally {
                lock.unlock();
            }
            return true;

        case RESPONSE_SESSION_BYE_OK:
            LOG.trace("receive RESPONSE_SESSION_BYE_OK");
            closeBoxes(true);
            return false;

        default:
            throw new IOException("invalid info in the response");

        }
    }

    private void closeBoxes(boolean intentionalClose) throws IOException {
        responseBox.doClose(intentionalClose);
        resultSetBox.close();
        socketClosed.set(true);
        socket.close();
    }

    public ResultSetBox getResultSetBox() {
        return resultSetBox;
    }

    private void send(byte i, int s) throws IOException {  // SESSION_HELLO, RESULT_SET_BYE_OK
        byte[] header = new byte[7];

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
        }
        LOG.trace("send {}, slot = {}", ((i == REQUEST_SESSION_HELLO) ? "SESSION_HELLO" : "RESULT_SET_BYE_OK"), s); //$NON-NLS-1$
    }

    @Override
    public void send(int s, byte[] frameHeader, byte[] payload, ChannelResponse channelResponse) {  // SESSION_PAYLOAD
        byte[] header = new byte[7];
        int length = frameHeader.length + payload.length;

        header[0] = REQUEST_SESSION_PAYLOAD;
        header[1] = strip(s);       // slot
        header[2] = strip(s >> 8);  // slot
        header[3] = strip(length);
        header[4] = strip(length >> 8);
        header[5] = strip(length >> 16);
        header[6] = strip(length >> 24);

        synchronized (outStream) {
            if (socket.isClosed()) {
                channelResponse.setMainResponse(new IOException("socket is already closed"));
                return;
            }
            try {
                outStream.write(header, 0, header.length);
                if (length > 0) {
                    // payload送信
                    outStream.write(frameHeader, 0, frameHeader.length);
                    outStream.write(payload, 0, payload.length);
                }
            } catch (IOException e) {
                channelResponse.setMainResponse(e);
                return;
            }
        }
        LOG.trace("send SESSION_PAYLOAD, length = {}, slot = {}", length, s);
    }

    byte strip(int i) {
        return (byte) (i & 0xff);
    }

    public LinkMessage receive() throws IOException, SocketTimeoutException {
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

    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        return new ResultSetWireImpl(this);
    }

    @Override
    public boolean isAlive() {
        if (closed.get()) {
            return false;
        }
        return !socket.isClosed();
    }

    @Override
    public void close() throws IOException, ServerException {
        if (!closed.getAndSet(true) && !socketError.get()) {
            try (var c1 = socket; var c2 = inStream; var c3 = outStream) {
                send(REQUEST_SESSION_BYE, 0);
                while (!socketClosed.get()) {
                    doPull(timeout, timeUnit);
                }
            } catch (TimeoutException e) {
                socketError.set(true);
                closeBoxes(false);
                throw new ResponseTimeoutException("close timeout in StreamLink", e);
            } catch (Exception e) {
                socketError.set(true);
                closeBoxes(false);
                throw e;
            }
        }
    }
}
