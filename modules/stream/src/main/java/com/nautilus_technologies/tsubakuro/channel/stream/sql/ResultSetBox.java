package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
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
    private boolean[] eor;
    private Map<String, Integer> map;

    public ResultSetBox(StreamWire streamWire) {
    this.streamWire = streamWire;
    this.queues = new MessageQueue[SIZE];
    this.eor = new boolean[SIZE];
    this.map = new HashMap<>();
    for (int i = 0; i < SIZE; i++) {
        eor[i] = false;
        queues[i] = new MessageQueue();
    }
    }

    public ResultSetResponse receive(int slot) throws IOException {
        while (true) {
            synchronized (queues[slot]) {
                if (!queues[slot].isEmpty()) {
                    return queues[slot].poll();
                }
                if (eor[slot]) {
                    return new ResultSetResponse(0, null);
                }
            }
            streamWire.pull();
        }
    }

    public byte hello(String name) throws IOException {
        while (true) {
            synchronized (map) {
                if (map.containsKey(name)) {
                    var slot = (byte) map.get(name).intValue();
                    map.remove(name);
                    return  slot;
                }
            }
            streamWire.pull();
        }
    }

    public void pushHello(String name, int slot) {  // for RESPONSE_RESULT_SET_HELLO
        synchronized (map) {
            if (map.containsKey(name)) {
                map.replace(name, slot);
            } else {
                map.put(name, slot);
            }
            eor[slot] = false;
            queues[slot].clear();
        }
    }

    public void push(int slot, int writerId, byte[] payload) {  // for RESPONSE_RESULT_SET_PAYLOAD
        synchronized (queues[slot]) {
            queues[slot].add(new ResultSetResponse(writerId, payload));
        }
    }
    
    public void pushBye(int slot) {  // for RESPONSE_RESULT_SET_BYE
        synchronized (queues[slot]) {
            eor[slot] = true;
        }
    }
}
