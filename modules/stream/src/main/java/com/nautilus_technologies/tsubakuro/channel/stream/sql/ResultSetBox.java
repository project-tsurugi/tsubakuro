package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.util.Objects;
import java.util.ArrayDeque;
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

    private StreamWire streamWire;
    private MessageQueue[] queues;
    private boolean[] inUse;
    private boolean[] eor;

    public ResultSetBox(StreamWire streamWire) {
	this.streamWire = streamWire;
	this.queues = new MessageQueue[SIZE];
	this.inUse = new boolean[SIZE];
	this.eor = new boolean[SIZE];
	for (int i = 0; i < SIZE; i++) {
	    inUse[i] = false;
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

    public void push(int slot, int info) {  // for RESPONSE_RESULT_SET_HELLO_[OK|NG}
	if (info == StreamWire.RESPONSE_RESULT_SET_HELLO_NG) {
	    System.err.println("RESPONSE_RESULT_SET_HELLO_NG at slot " + slot);
	}
	queues[slot].add(new ResultSetResponse(info));
    }
    
    public void push(int slot, int writerId, byte[] payload) {  // for RESPONSE_RESULT_SET_PAYLOAD
	if (Objects.nonNull(payload)) {
	    queues[slot].add(new ResultSetResponse(writerId, payload));
	} else {
	    eor[slot] = true;
	}
    }

    public void release(int slot) {
	inUse[slot] = false;
    }

    public byte lookFor() {
	synchronized (this) {
	    for (byte i = 0; i < SIZE; i++) {
		if (!inUse[i]) {
		    inUse[i] = true;
		    eor[i] = false;
		    queues[i].clear();
		    return i;
		}
	    }
	    System.err.println("ResultSetBox has been exhausted");
	    return -1;
	}
    }
}
