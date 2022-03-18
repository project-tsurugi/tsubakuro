package com.nautilus_technologies.tsubakuro.channel.stream;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

public class StreamWire {
    public byte[] bytes;

    private String hostname;
    private int port;
    private Socket socket;
    private DataOutputStream outStream;
    private DataInputStream inStream;
    private byte info;
    private boolean valid;
    private boolean closed;

    public static final byte STATUS_OK = 0;
    public static final byte STATUS_NG = 1;
    private static final byte SESSION = 1;
    private static final byte RESULT_SET = 2;

    public StreamWire(String hostname, int port) {
	this.hostname = hostname;
	this.port = port;
	this.valid = false;
	this.closed = false;
    }

    public void connect() throws IOException {
    	socket = new Socket(hostname, port);
	outStream = new DataOutputStream(socket.getOutputStream());
	inStream = new DataInputStream(socket.getInputStream());
    }

    public void hello() throws IOException {
	send(SESSION);
    }
    public void hello(String name) throws IOException {
	send(RESULT_SET, name);
    }
    
    public void send(byte num) throws IOException {
	// num送信
	outStream.writeByte(num);

	// length送信
	outStream.writeByte(0);
	outStream.writeByte(0);
	outStream.writeByte(0);
	outStream.writeByte(0);
    }
    public void send(byte num, byte[] data) throws IOException {
	int length = (int) data.length;

	// num送信
	outStream.writeByte(num);

	// length送信
	outStream.writeByte(length);
	outStream.writeByte(length >> 8);
	outStream.writeByte(length >> 16);
	outStream.writeByte(length >> 24);
			
	if (length > 0) {
	    // payload送信
	    outStream.write(data, 0, length);
	}
    }
    public void send(byte num, String data) throws IOException {
	int length = (int) data.length();

	// num送信
	outStream.writeByte(num);

	// length送信
	outStream.writeByte(length);
	outStream.writeByte(length >> 8);
	outStream.writeByte(length >> 16);
	outStream.writeByte(length >> 24);
			
	if (length > 0) {
	    // payload送信
	    outStream.writeBytes(data);
	}
    }

    public boolean receive() throws IOException {
	int length;
	if (valid) {
	    System.err.println("previous data is alive");
	}
	try {
	    // info受信
	    info = inStream.readByte();

	    // length受信
	    length = 0;
	    for (int i = 0; i < 4; i++) {
		int inData = inStream.readByte();
		length |= inData << (i * 8);
	    }
	    if (length > 0) {
		// payload受信
		bytes = new byte[length];
		int size = 0;
		while (size < length) {
		    size += inStream.read(bytes, size, length - size);
		}
	    }
	    valid = true;
	} catch (EOFException e) {
	    socket.close();
	    closed = true;
	    return false;
	}
	return true;
    }

    public byte getInfo() {
	if (!valid) {
	    System.err.println("received data has been disposed");
	}
	return info;
    }
    public byte[] getBytes() {
	if (!valid) {
	    System.err.println("received data has been disposed");
	}
	return bytes;
    }
    public String getString() {
	if (!valid) {
	    System.err.println("received data has been disposed");
	}
	try {
	    return new String(bytes, "UTF-8");
	} catch (UnsupportedEncodingException e) {
	    // As long as only alphabetic and numeric characters are received,
	    // this exception will never occur.
	    System.err.println(e);
	    e.printStackTrace();
	}
	return "";
    }

    public void release() {
	if (!valid) {
	    System.err.println("already released");
	}
	valid = false;
    }

    public void close() throws IOException {
	if (!closed) {
	    socket.close();
	    closed = true;
	}
    }
}
