package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;

/**
 * ResultSetBox type.
 */
public class ResultSetBox {
    private static final int SIZE = 16;

    private static class MessageQueue {
	ArrayDeque<ResultSetResponse> queue;

	MessageQueue() {
	    this.queue = new ArrayDeque<ResultSetResponse>();
	}
	public boolean isEmpty() {
	    return queue.isEmpty();
	}
	public void add(ResultSetResponse e) {
	    queue.add(e);
	}
	public ResultSetResponse poll() {
	    return queue.poll();
	}
	public void clear() {
	    queue.clear();
	}
    }

    private static class ResultSetNameEntry {
	private String name;
	private byte slot;

	ResultSetNameEntry(String name, byte slot) {
	    this.name = name;
	    this.slot = slot;
	}
	public String getName() {
	    return name;
	}
	public byte getSlot() {
	    return slot;
	}
    }

    private StreamWire streamWire;
    private MessageQueue[] queues;
    private boolean[] eor;
    private List<ResultSetNameEntry> list;
    private String nameSaved;
    private byte slotSaved;

    public ResultSetBox(StreamWire streamWire) {
	this.streamWire = streamWire;
	this.queues = new MessageQueue[SIZE];
	this.eor = new boolean[SIZE];
	this.list = new ArrayList<>();
	this.nameSaved = null;
	for (int i = 0; i < SIZE; i++) {
	    eor[i] = false;
	    queues[i] = new MessageQueue();
	}
    }

    public ResultSetResponse receive(int slot) throws IOException {
	while (true) {
	    if (!queues[slot].isEmpty()) {
		return queues[slot].poll();
	    }
	    if (eor[slot]) {
		return new ResultSetResponse(0, null);
	    }
	    streamWire.pull();
	}
    }

    public byte hello(String name) throws IOException {
	while (true) {
	    if (Objects.nonNull(nameSaved)) {
		if (name.equals(nameSaved)) {
		    nameSaved = null;
		    return slotSaved;
		}
	    }
	    for (int i = 0; i < list.size(); i++) {
		var entry = list.get(i);
		if (name.equals(entry.getName())) {
		    list.remove(i);
		    return (byte) entry.getSlot();
		}
	    }
	    streamWire.pull();
	}
    }

    public void pushHello(String name, byte slot) {  // for RESPONSE_RESULT_SET_HELLO
	if (Objects.isNull(nameSaved)) {
	    nameSaved = name;
	    slotSaved = slot;
	} else {
	    list.add(new ResultSetNameEntry(name, slot));
	}
	eor[slot] = false;
	queues[slot].clear();
    }

    public void push(int slot, int writerId, byte[] payload) {  // for RESPONSE_RESULT_SET_PAYLOAD
	queues[slot].add(new ResultSetResponse(writerId, payload));
    }

    public void pushBye(int slot) {  // for RESPONSE_RESULT_SET_BYE
	eor[slot] = true;
    }
}
