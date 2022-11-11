package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

class Queues {
    private final ConcurrentLinkedQueue<SlotEntry> slotQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<RequestEntry> requestQueue = new ConcurrentLinkedQueue<>();
    private final Link link;
    private final Lock lock = new ReentrantLock();

    Queues(@Nonnull Link link) {
        this.link = link;
    }

    void addSlot(SlotEntry slotEntry) {
        slotEntry.resetChannelResponse();
        slotQueue.add(slotEntry);
    }
    SlotEntry pollSlot() {
        return slotQueue.poll();
    }
    boolean isSlotEmpty() {
        return slotQueue.isEmpty();
    }

    void addRequest(RequestEntry requestEntry) {
        requestQueue.add(requestEntry);
    }
    RequestEntry pollRequest() {
        return requestQueue.poll();
    }
    boolean isRequestEmpty() {
        return requestQueue.isEmpty();
    }

    void pairAnnihilation() {
        if (!lock.tryLock()) {
            return;
        }
        try {
            while (true) {
                var slotEntry = slotQueue.poll();
                if (Objects.isNull(slotEntry)) {
                    return;
                }
                var requestEntry = requestQueue.poll();
                if (Objects.isNull(requestEntry)) {
                    slotQueue.add(slotEntry);
                    return;
                }
                var channelResponse = requestEntry.channelResponse();
                slotEntry.channelResponse(channelResponse);
                link.send(slotEntry.slot(), requestEntry.header(), requestEntry.payload(), channelResponse);
            } 
        } finally {
            lock.unlock();
        }
    }
}
