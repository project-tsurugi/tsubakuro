package com.tsurugidb.tsubakuro.channel.stream;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.LinkMessage;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetBox;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetWireImpl;

public final class StreamLink extends Link {
    private Socket socket;
    private DataOutputStream outStream;
    private DataInputStream inStream;
    private ResultSetBox resultSetBox = new ResultSetBox();
    private Receiver receiver;
    private final AtomicReference<LinkMessage> helloResponse = new AtomicReference<>();
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private boolean valid = false;
    private boolean closed = false;

    private static final byte REQUEST_SESSION_HELLO = 1;
    private static final byte REQUEST_SESSION_PAYLOAD = 2;
    private static final byte REQUEST_RESULT_SET_BYE_OK = 3;

    public static final byte RESPONSE_SESSION_PAYLOAD = 1;
    public static final byte RESPONSE_RESULT_SET_PAYLOAD = 2;
    public static final byte RESPONSE_SESSION_HELLO_OK = 3;
    public static final byte RESPONSE_SESSION_HELLO_NG = 4;
    public static final byte RESPONSE_RESULT_SET_HELLO = 5;
    public static final byte RESPONSE_RESULT_SET_BYE = 6;
    public static final byte RESPONSE_SESSION_BODYHEAD = 7;

    static final Logger LOG = LoggerFactory.getLogger(StreamLink.class);

    private class Receiver extends Thread {
        public void run() {
            try {
                while (true) {
                    if (!pull()) {
                        break;
                    }
                }
            } catch (IOException e) {
                System.err.println(e);
                e.printStackTrace();
            }
        }
    }

    public StreamLink(String hostname, int port) throws IOException {
        this.socket = new Socket(hostname, port);
        this.outStream = new DataOutputStream(socket.getOutputStream());
        this.inStream = new DataInputStream(socket.getInputStream());
        this.receiver = new Receiver();
        this.helloResponse.set(null);
        this.receiver.start();
    }

    public void hello() throws IOException {
        send(REQUEST_SESSION_HELLO, ResponseBox.responseBoxSize());
    }

    public LinkMessage helloResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        lock.lock();
        try {
            while (Objects.isNull(helloResponse.get())) {
                if (timeout != 0) {
                    if (!condition.await(timeout, unit)) {
                        throw new TimeoutException("server has not responded to the request within the specified time");
                    }
                } else {
                    condition.await();
                }
            }
            return helloResponse.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    private boolean pull() throws IOException {
        var message = receive();
        if (Objects.isNull(message)) {
            return false;
        }

        byte info = message.getInfo();
        int slot = message.getSlot();
        switch (info) {

        case RESPONSE_SESSION_PAYLOAD:
            LOG.trace("receive SESSION_PAYLOAD, slot = {}", slot);
            responseBox.push(slot, message.getBytes(), false);
            return true;

        case RESPONSE_SESSION_BODYHEAD:
            LOG.trace("receive RESPONSE_SESSION_BODYHEAD, slot = {}", slot);
            responseBox.push(slot, message.getBytes(), true);
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
            resultSetBox.pushBye(slot);
            return true;

        case RESPONSE_SESSION_HELLO_OK:
        case RESPONSE_SESSION_HELLO_NG:
            LOG.trace("receive SESSION_HELLO_{}", ((info == RESPONSE_SESSION_HELLO_OK) ? "OK" : "NG"));
            lock.lock();
            try {
                helloResponse.set(message);
                condition.signal();
            } finally {
                lock.unlock();
            }
            return true;

        default:
            throw new IOException("invalid info in the response");

        }
    }

    public ResultSetBox getResultSetBox() {
        return resultSetBox;
    }

    public void sendResutSetByeOk(int slot) throws IOException {
        send(REQUEST_RESULT_SET_BYE_OK, slot);
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
            outStream.write(header, 0, header.length);
        }
        LOG.trace("send {}, slot = {}", ((i == REQUEST_SESSION_HELLO) ? "SESSION_HELLO" : "RESULT_SET_BYE_OK"), s); //$NON-NLS-1$
    }

    @Override
    public void send(int s, byte[] frameHeader, byte[] payload) throws IOException {  // SESSION_PAYLOAD
        byte[] header = new byte[7];
        int length = (int) (frameHeader.length + payload.length);

        header[0] = REQUEST_SESSION_PAYLOAD;
        header[1] = strip(s);       // slot
        header[2] = strip(s >> 8);  // slot
        header[3] = strip(length);
        header[4] = strip(length >> 8);
        header[5] = strip(length >> 16);
        header[6] = strip(length >> 24);

        synchronized (outStream) {
            outStream.write(header, 0, header.length);
            if (length > 0) {
                // payload送信
                outStream.write(frameHeader, 0, frameHeader.length);
                outStream.write(payload, 0, payload.length);
            }
        }
        LOG.trace("send SESSION_PAYLOAD, length = {}, slot = {}", length, s);
    }

    byte strip(int i) {
        return (byte) (i & 0xff);
    }

    public LinkMessage receive() throws IOException {
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
                int size = 0;
                while (size < length) {
                    size += inStream.read(bytes, size, length - size);
                }
            } else {
                bytes = null;
            }
            return new LinkMessage(info, bytes, slot, writer);
        } catch (SocketException | EOFException e) {  // imply session close
            socket.close();
            closed = true;
            return null;
        }
    }

    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        return new ResultSetWireImpl(this);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            socket.close();
            closed = true;
        }
        try {
            receiver.join();
        } catch (InterruptedException e) {
            System.err.println(e);
            e.printStackTrace();
        }
    }
}
