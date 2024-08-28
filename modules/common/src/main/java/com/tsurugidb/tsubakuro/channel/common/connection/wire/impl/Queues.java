/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

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
                if (slotEntry == null) {
                    return;
                }
                var requestEntry = requestQueue.poll();
                if (requestEntry == null) {
                    slotQueue.add(slotEntry);
                    return;
                }
                var channelResponse = requestEntry.channelResponse();
                if (channelResponse.assignSlot(slotEntry.slot())) {
                    slotEntry.channelResponse(channelResponse);
                    slotEntry.requestMessage(requestEntry.payload());
                    link.send(slotEntry.slot(), requestEntry.header(), requestEntry.payload(), channelResponse);
                } else {
                    channelResponse.cancelSuccessWithoutServerInteraction();
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
