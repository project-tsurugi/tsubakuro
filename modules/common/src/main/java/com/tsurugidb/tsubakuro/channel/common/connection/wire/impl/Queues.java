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

import javax.annotation.Nonnull;

class Queues {
    // slot queue, not guarded
    private final ConcurrentLinkedQueue<SlotEntry> slotQueue = new ConcurrentLinkedQueue<>();
    // request queue, guarded by 'this'
    private final ConcurrentLinkedQueue<RequestEntry> requestQueue = new ConcurrentLinkedQueue<>();
    private final Link link;

    Queues(@Nonnull Link link) {
        this.link = link;
    }

    void addSlot(SlotEntry slotEntry) {
        slotQueue.add(slotEntry);
    }

    void returnSlot(SlotEntry slotEntry) {
        slotEntry.resetChannelResponse();
        slotQueue.add(slotEntry);
        if (requestQueue.peek() != null) {
            synchronized (this) {
                var slotEntryAfter = slotQueue.poll();
                var requestEntry = requestQueue.poll();
                if (requestEntry != null && slotEntryAfter != null) {
                    pairAnnihilation(slotEntryAfter, requestEntry);
                }
            }
        }
    }

    SlotEntry pollSlot() {
        if (requestQueue.peek() != null) {
            synchronized (this) {
                while (true) {
                    if (requestQueue.peek() != null) {
                        var slotEntry = slotQueue.poll();
                        if (slotEntry != null) {
                            pairAnnihilation(slotEntry, requestQueue.poll());
                            continue;
                        }
                    }
                    break;
                }
            }
        }
        return slotQueue.poll();
    }

    synchronized void queueRequest(RequestEntry requestEntry) {
        var slotEntrybefore = slotQueue.poll();
        if (slotEntrybefore != null) {
            pairAnnihilation(slotEntrybefore, requestEntry);
            return;
        }
        requestQueue.add(requestEntry);
        var slotEntryAfter = slotQueue.poll();
        if (slotEntryAfter != null) {
            pairAnnihilation(slotEntryAfter, requestQueue.poll());  // requestQueue is not empty
        }
    }

    void pairAnnihilation(@Nonnull SlotEntry slotEntry, @Nonnull RequestEntry requestEntry) {
        var channelResponse = requestEntry.channelResponse();
        if (channelResponse.canAssignSlot()) {
            slotEntry.channelResponse(channelResponse);
            slotEntry.requestMessage(requestEntry.payload());
            link.sendInternal(slotEntry.slot(), requestEntry.header(), requestEntry.payload(), channelResponse);
            channelResponse.finishAssignSlot(slotEntry.slot());
            return;
        }
        throw new AssertionError("channelResponse slot is not assignable");
    }
}