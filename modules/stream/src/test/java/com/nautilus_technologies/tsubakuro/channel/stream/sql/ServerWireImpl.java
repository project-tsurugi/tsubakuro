package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Objects;
import java.util.ArrayDeque;
import com.nautilus_technologies.tsubakuro.channel.stream.ServerStreamWire;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * ServerWireImpl type.
 */
public class ServerWireImpl implements Closeable {
    private ServerStreamWire serverStreamWire;
    private ArrayDeque<Message> receiveQueue;
    private ReceiveWorker receiver;
    private ArrayDeque<Message> sendQueue;
    private SendWorker sender;
    private long sessionID;

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
	private int port;
	ServerSocket serverSocket;

	ReceiveWorker(int port) {
	    this.port = port;
	}
	public void run() {
	    try {
		serverSocket = new ServerSocket(port);

		serverStreamWire = new ServerStreamWire(serverSocket.accept());
		//		System.out.println("==== accepted ====");
		while (serverStreamWire.receive()) {
		    if (serverStreamWire.getInfo() == 3) {  // StreamWire.REQUEST_RESULT_SET_HELLO = 3
			serverStreamWire.sendRecordHelloOk(serverStreamWire.getSlot());
			if (Objects.nonNull(sender)) {
			    sender.notifyEvent();
			}
		    } else {
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
	SendWorker() {
	}
	public void run() {
	    try {
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

    public void close() throws IOException {
	serverStreamWire.close();
	receiver.close();
    }

    public long getSessionID() {
	return sessionID;
    }

    /**
     * Get RequestProtos.Request from a client via the native wire.
     @returns RequestProtos.Request
    */
    public RequestProtos.Request get() throws IOException {
	try {
	    while (true) {
		if (!receiveQueue.isEmpty()) {
		    return RequestProtos.Request.parseFrom(receiveQueue.poll().getBytes());
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
     * Put ResponseProtos.Response to the client via the native wire.
     @param request the ResponseProtos.Response message
    */
    public void put(ResponseProtos.Response response) throws IOException {
	serverStreamWire.sendResponse(0, response.toByteArray());
    }

    public long createRSL(String name) throws IOException {
	sender = new SendWorker();
	sender.start();
	return 0;
    }

    public void putRecordsRSL(long handle, byte[] ba) throws IOException {
	//	serverStreamWire.sendRecord(0, 0, ba);
	sendQueue.add(new Message(ba));
	sender.notifyEvent();
    }

    public void eorRSL(long handle) throws IOException {
	byte[] ba = new byte[0];
	//	serverStreamWire.sendRecord(0, 0, ba);
	sendQueue.add(new Message(ba));
	sender.notifyEvent();
    }
}