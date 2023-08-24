package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * ResultSetBox type.
 */
public class ResultSetBox {
    private static final int SIZE = ResponseBox.responseBoxSize();

    private ResultSetWireImpl[] boxes = new ResultSetWireImpl[SIZE];
    private Map<String, Integer> map = new HashMap<>();;
    private Lock lock = new ReentrantLock();
    private Condition availableCondition = lock.newCondition();
    private Lock[] slotLock = new ReentrantLock[SIZE];
    private Condition[] slotCondition = new Condition[SIZE];
    private boolean intentionalClose = false;

    public ResultSetBox() {
        for (int i = 0; i < SIZE; i++) {
            slotLock[i] = new ReentrantLock();
            slotCondition[i] = slotLock[i].newCondition();
        }
    }

    public void register(String name, ResultSetWireImpl resultSetWire) throws IOException {
        while (true) {
            lock.lock();
            try {
                if (map.containsKey(name)) {
                    var slot = (byte) map.get(name).intValue();
                    map.remove(name);
                    Lock l =  slotLock[slot];
                    l.lock();
                    try {
                        boxes[slot] = resultSetWire;
                        slotCondition[slot].signal();
                    } finally {
                        l.unlock();
                    }
                    return;
                }
                availableCondition.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    public void pushHello(String name, int slot) throws IOException {  // for RESPONSE_RESULT_SET_HELLO
        lock.lock();
        try {
            if (map.containsKey(name)) {
                map.replace(name, slot);
            } else {
                map.put(name, slot);
            }
            availableCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void push(int slot, int writerId, byte[] payload) throws IOException {  // for RESPONSE_RESULT_SET_PAYLOAD
        if (boxes[slot] == null) {
            waitRegistration(slot);
        }
        boxes[slot].add(writerId, payload);
    }

    public void pushBye(int slot) throws IOException {  // for RESPONSE_RESULT_SET_BYE
        if (boxes[slot] == null) {
            waitRegistration(slot);
        }
        var box = boxes[slot];
        boxes[slot] = null;
        box.endOfRecords();
    }

    public void pushBye(int slot, IOException e) throws IOException {  // for RESPONSE_RESULT_SET_BYE
        if (boxes[slot] == null) {
            waitRegistration(slot);
        }
        var box = boxes[slot];
        boxes[slot] = null;
        box.endOfRecords(e);
    }

    private void waitRegistration(int slot) throws IOException {
        while (true) {
            Lock l =  slotLock[slot];
            l.lock();
            try {
                if (boxes[slot] != null) {
                    return;
                }
                slotCondition[slot].await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                l.unlock();
            }
        }
    }

    public void doClose(boolean ic) {
        intentionalClose = ic;
        close();
    }

    public void close() {
        for (ResultSetWireImpl e : boxes) {
            if (e != null) {
                if (intentionalClose) {
                    e.endOfRecords(new IOException("The wire was closed before closing this resultSet"));
                } else {
                    e.endOfRecords(new IOException("Server crashed"));
                }
            }
        }
    }
}
