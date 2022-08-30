package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.channel.stream.StreamWire;

/**
 * ResultSetBox type.
 */
public class ResultSetBox {
    private static final int SIZE = ResponseBox.responseBoxSize();

    private StreamWire streamWire;
    private Abox[] boxes;
    private Map<String, Integer> map;
    private AtomicBoolean available;

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

    private static class Abox {
        private AtomicBoolean available;
        private MessageQueue queues;
        private boolean eor;

        Abox() {
            this.available = new AtomicBoolean();
            this.available.set(false);
            this.eor = false;
            this.queues = new MessageQueue();
        }
    }

    public ResultSetBox(StreamWire streamWire) {
        this.streamWire = streamWire;
        this.map = new HashMap<>();
        this.available = new AtomicBoolean();
        this.available.set(false);
        this.boxes = new Abox[SIZE];

        for (int i = 0; i < SIZE; i++) {
            boxes[i] = new Abox();
        }
    }

    public ResultSetResponse receive(int slot) throws IOException {
        while (true) {
            synchronized (boxes[slot]) {
                if (!boxes[slot].queues.isEmpty()) {
                    return boxes[slot].queues.poll();
                }
                if (boxes[slot].eor) {
                    return new ResultSetResponse(0, null);
                }
            }
            streamWire.pull(boxes[slot].available);
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
                available.set(false);
            }
            streamWire.pull(available);
        }
    }

    public void pushHello(String name, int slot) {  // for RESPONSE_RESULT_SET_HELLO
        synchronized (map) {
            if (map.containsKey(name)) {
                map.replace(name, slot);
            } else {
                map.put(name, slot);
            }
            available.set(true);
            synchronized (boxes[slot]) {
                boxes[slot].eor = false;
                boxes[slot].queues.clear();
            }
        }
    }

    public void push(int slot, int writerId, byte[] payload) {  // for RESPONSE_RESULT_SET_PAYLOAD
        synchronized (boxes[slot]) {
            boxes[slot].queues.add(new ResultSetResponse(writerId, payload));
            boxes[slot].available.set(true);
        }
    }
    
    public void pushBye(int slot) {  // for RESPONSE_RESULT_SET_BYE
        synchronized (boxes[slot]) {
            boxes[slot].eor = true;
            boxes[slot].available.set(true);
        }
    }
}
