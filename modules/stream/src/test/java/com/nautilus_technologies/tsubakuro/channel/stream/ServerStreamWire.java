package com.nautilus_technologies.tsubakuro.channel.stream;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.util.Objects;
import java.net.Socket;
import java.net.SocketException;

public class ServerStreamWire {
    private Socket socket;
    private DataOutputStream outStream;
    private DataInputStream inStream;
    private boolean sendOk;

    public byte[] bytes;
    private byte info;
    private byte slot;

    public ServerStreamWire(Socket socket) throws IOException {
	this.socket = socket;
        this.outStream = new DataOutputStream(socket.getOutputStream());
        this.inStream = new DataInputStream(socket.getInputStream());
        this.sendOk = false;
    }

    public void sendResponse(int s, byte[] payload) throws IOException {
	byte[] header = new byte[6];
        int length = (int) payload.length;
	//	System.out.println("sendResponse " + length + " bytes, slot = " + s);

	header[0] = StreamWire.RESPONSE_SESSION_PAYLOAD;  // info
	header[1] = strip(s);  // slot
	header[2] = strip(length);
	header[3] = strip(length >> 8);
	header[4] = strip(length >> 16);
	header[5] = strip(length >> 24);

	synchronized (this) {
	    outStream.write(header, 0, header.length);

            if (length > 0) {
                // payload送信
                outStream.write(payload, 0, length);
            }
        }
    }

    public void sendRecordHelloOk(int s) throws IOException {
	byte[] header = new byte[6];
        int length = 0;
	//	System.out.println("sendRecordHelloOk, slot = " + s);

	header[0] = StreamWire.RESPONSE_RESULT_SET_HELLO_OK;  // info
	header[1] = strip(s);  // slot
	header[2] = strip(length);
	header[3] = strip(length >> 8);
	header[4] = strip(length >> 16);
	header[4] = strip(length >> 24);

	synchronized (this) {
	    outStream.write(header, 0, header.length);
        }
	sendOk = true;
    }

    public void sendRecord(int s, int w, byte[] payload) throws IOException {
	byte[] header = new byte[7];
        int length = (int) payload.length;
	//	System.out.println("sendRecord " + length + " bytes, slot = " + s + ", writer = " + w);

	header[0] = StreamWire.RESPONSE_RESULT_SET_PAYLOAD;  // info
	header[1] = strip(s);  // slot
	header[2] = strip(w);  // slot
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
	    slot = inStream.readByte();

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
            socket.close();
	    socket = null;
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
    public byte getSlot() {
	return slot;
    }
    public byte[] getBytes() {
	return bytes;
    }

    public void close() throws IOException {
	if (Objects.nonNull(socket)) {
	    socket.close();
	    socket = null;
	}
    }
}
