package com.tsurugidb.tsubakuro.channel.stream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class ServerStreamLink {
    private Socket socket;
    private DataOutputStream outStream;
    private DataInputStream inStream;
    private boolean sendOk;

    public byte[] bytes;
    private byte info;
    private int slot;

    public ServerStreamLink(Socket socket) throws IOException {
        this.socket = socket;
        this.outStream = new DataOutputStream(socket.getOutputStream());
        this.inStream = new DataInputStream(socket.getInputStream());
        this.sendOk = false;
    }

    public void sendResponseHelo() throws IOException {
        byte[] header = new byte[7];

        header[0] = StreamLink.RESPONSE_SESSION_HELLO_OK;  // info
        header[1] = 0;
        header[2] = 0;
        header[3] = 0;
        header[4] = 0;
        header[5] = 0;
        header[6] = 0;

        synchronized (this) {
            outStream.write(header, 0, header.length);
        }
    }

    public void sendResponse(int s, byte[] payload) throws IOException {
        byte[] header = new byte[7];
        int length = payload.length;
        //    System.out.println("sendResponse " + length + " bytes, slot = " + s);

        header[0] = StreamLink.RESPONSE_SESSION_PAYLOAD;  // info
        header[1] = strip(s);       // slot
        header[2] = strip(s >> 8);  // slot
        header[3] = strip(length);
        header[4] = strip(length >> 8);
        header[5] = strip(length >> 16);
        header[6] = strip(length >> 24);

        synchronized (this) {
            outStream.write(header, 0, header.length);

            if (length > 0) {
                // payload送信
                outStream.write(payload, 0, length);
            }
        }
    }

    public void sendRecordHello(int s, String name) throws IOException {
    byte[] header = new byte[7];
    byte[] payload = name.getBytes("UTF-8");
    int length = payload.length;
    //    System.out.println("sendRecordByeOk, slot = " + s);

    header[0] = StreamLink.RESPONSE_RESULT_SET_HELLO;  // info
    header[1] = strip(s);       // slot
    header[2] = strip(s >> 8);  // slot
    header[3] = strip(length);
    header[4] = strip(length >> 8);
    header[5] = strip(length >> 16);
    header[6] = strip(length >> 24);

    synchronized (this) {
        outStream.write(header, 0, header.length);
        outStream.write(payload, 0, length);
    }
    sendOk = true;
    }

    public void sendRecord(int s, int w, byte[] payload) throws IOException {
    byte[] header = new byte[8];
        int length = payload.length;
    //    System.out.println("sendRecord " + length + " bytes, slot = " + s + ", writer = " + w);

    header[0] = StreamLink.RESPONSE_RESULT_SET_PAYLOAD;  // info
    header[1] = strip(s);       // slot
    header[2] = strip(s >> 8);  // slot
    header[3] = strip(w);  // writer
    header[4] = strip(length);
    header[5] = strip(length >> 8);
    header[6] = strip(length >> 16);
    header[7] = strip(length >> 24);

    synchronized (this) {
        outStream.write(header, 0, header.length);

            if (length > 0) {
                // payload送信
                outStream.write(payload, 0, length);
            }
        }
    }

    byte strip(int i) {
    return (byte) (i & 0xff);
    }

    public boolean isSnedOk() {
    return sendOk;
    }

    public boolean receive() throws IOException {
        try {
            byte writer = 0;

            // info受信
            info = inStream.readByte();

            // slot受信
            slot = 0;
            for (int i = 0; i < 2; i++) {
                int inData = inStream.readByte() & 0xff;
                slot |= inData << (i * 8);
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
        } catch (EOFException e) {  // imply session close
            if (socket != null) {
                socket.close();
                socket = null;
            }
            return false;
        } catch (SocketException e) {
            socket = null;
            return false;
        }
        return true;
    }

    public byte getInfo() {
        return info;
    }
    public int getSlot() {
        return slot;
    }
    public byte[] getBytes() {
        return bytes;
    }

    public void close() throws IOException {
    if (socket != null) {
        socket.close();
        socket = null;
    }
    }
}
