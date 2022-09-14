package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import com.tsurugidb.tsubakuro.channel.stream.ResponseBox;

/**
 * ResultSetBox type.
 */
public class ResultSetBox {
    private static final int SIZE = ResponseBox.responseBoxSize();

    private Abox[] boxes;
    private Map<String, Integer> map;
    private Lock lock = new ReentrantLock();
    private Condition availableCondition = lock.newCondition();

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
        private Lock lock = new ReentrantLock();
        private Condition availableCondition = lock.newCondition();
        private MessageQueue queues = new MessageQueue();
        private boolean eor;

        Abox() {
            this.eor = false;
        }
    }

    public ResultSetBox() {
        this.map = new HashMap<>();
        this.boxes = new Abox[SIZE];

        for (int i = 0; i < SIZE; i++) {
            boxes[i] = new Abox();
        }
    }

    public ResultSetResponse receive(int slot) throws IOException {
        while (true) {
            Lock l =  boxes[slot].lock;
            l.lock();
            try {
                if (!boxes[slot].queues.isEmpty()) {
                    return boxes[slot].queues.poll();
                }
                if (boxes[slot].eor) {
                    return new ResultSetResponse(0, null);
                }
                boxes[slot].availableCondition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                l.unlock();
            }
        }
    }

    public byte hello(String name) throws IOException {
        while (true) {
            lock.lock();
            try {
                if (map.containsKey(name)) {
                    var slot = (byte) map.get(name).intValue();
                    map.remove(name);
                    return slot;
                }
                availableCondition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    public void pushHello(String name, int slot) {  // for RESPONSE_RESULT_SET_HELLO
        lock.lock();
        try {
            if (map.containsKey(name)) {
                map.replace(name, slot);
            } else {
                map.put(name, slot);
            }
            boxes[slot].eor = false;
            boxes[slot].queues.clear();
            availableCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void push(int slot, int writerId, byte[] payload) {  // for RESPONSE_RESULT_SET_PAYLOAD
        Lock l =  boxes[slot].lock;
        l.lock();
        try {
            boxes[slot].queues.add(new ResultSetResponse(writerId, payload));
            boxes[slot].availableCondition.signal();
        } finally {
            l.unlock();
        }
    }
    
    public void pushBye(int slot) {  // for RESPONSE_RESULT_SET_BYE
        Lock l =  boxes[slot].lock;
        l.lock();
        try {
            boxes[slot].eor = true;
            boxes[slot].availableCondition.signal();
        } finally {
            l.unlock();
        }
    }
}
