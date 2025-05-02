/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.channel.stream.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;

class ResultSetWireTest {
    static final int SERVICE_ID_SQL = 3;
    private static final String HOST = "localhost";
    private static final int PORT = 12334;

    private final String NAME = "resultset";
    private final int COUNT = 128;

    private StreamLink link;
    private WireImpl client;
    private ServerWireImpl server;
    private final long sessionId = 1;
    private long serverResultSetWire;
    private int writeCount;

    private class Sender extends Thread {
        private boolean doSend;
        private boolean finish = false;

        Sender() {
            this.doSend = true;
        }
        Sender(boolean doSend) {
            this.doSend = doSend;
        }
        public void run() {
            byte[] ba = new byte[1024];
            try {
                try {
                    Thread.sleep(100);
                } catch(InterruptedException e) {
                    // do nothing
                }
                if (doSend) {
                    writeCount = 0;
                    for (int i = 0; i < COUNT; i++) {
                        server.putRecordsRSL(serverResultSetWire, ba);
                        writeCount++;
                    }
                    server.eorRSL(serverResultSetWire);
                    try {
                        Thread.sleep(100);
                    } catch(InterruptedException e) {
                        // do nothing
                    }
                } else {
                    while (!finish) {
                        try {
                            Thread.sleep(100);
                        } catch(InterruptedException e) {
                            // do nothing
                        }
                    }
                }
            } catch(IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        void finish() {
            this.finish = true;
        }
    }

    @Test
    void readRecordsTest() throws Exception {
        try {
            server = new ServerWireImpl(PORT - 1, sessionId);
            link = new StreamLink(HOST, PORT - 1);
            client = new WireImpl(link);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
        link.pullMessage(link.messageNumber(), 0, null);
        var sender = new Sender();
        sender.start();

        var clientResultSetWire = client.createResultSetWire();
        clientResultSetWire.connect(NAME);
        var recordStream = clientResultSetWire.getByteBufferBackedInput();
        byte[] ba = new byte[1024];
        int readBytes = 0;
        while (true) {
            int s = recordStream.read(ba);
            if (s == -1) {
                break;
            }
            readBytes += s;
        }
        clientResultSetWire.close();
        sender.join();
        assertEquals(COUNT * 1024, readBytes);
        assertTrue(server.isReceiveResultSetByeOk());
    }

    @Test
    void serverCrashDetectionTest() throws Exception {
        try {
            server = new ServerWireImpl(PORT - 2, sessionId);
            link = new StreamLink(HOST, PORT - 2);
            client = new WireImpl(link);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
        link.pullMessage(link.messageNumber(), 0, null);
        var sender = new Sender(false);
        sender.start();

        var clientResultSetWire = client.createResultSetWire();
        clientResultSetWire.connect(NAME);
        var recordStream = clientResultSetWire.getByteBufferBackedInput();
        byte[] ba = new byte[1024];

        server.closeSocket();

        Throwable exception = assertThrows(IOException.class, () -> {
            recordStream.read(ba);
            clientResultSetWire.close();
        });
        // FIXME: check error code instead of message
        assertEquals(0, exception.getMessage().indexOf("lost connection"));

        sender.finish();
        sender.join();
    }

    @Test
    void timeoutDetectionTest() throws Exception {
        try {
            server = new ServerWireImpl(PORT - 3, sessionId);
            link = new StreamLink(HOST, PORT - 3);
            client = new WireImpl(link);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
        link.pullMessage(link.messageNumber(), 0, null);
        var sender = new Sender(false);
        sender.start();

        var clientResultSetWire = client.createResultSetWire();
        clientResultSetWire.connect(NAME);
        var recordStream = clientResultSetWire.getByteBufferBackedInput();
        if (recordStream instanceof ResultSetWire.ByteBufferBackedInput) {
            ((ResultSetWire.ByteBufferBackedInput) recordStream).setTimeout(500, TimeUnit.MILLISECONDS);
        } else {
            fail("inconsistent class");
        }
        byte[] ba = new byte[1024];

        Throwable exception = assertThrows(InterruptedIOException.class, () -> {
            recordStream.read(ba);
            clientResultSetWire.close();
        });
        // FIXME: check error code instead of message
        assertEquals(0, exception.getMessage().indexOf("response has not been received within the specified time"));

        sender.finish();
        sender.join();
    }

    @Test
    void closeWithRecordRemainTest() throws Exception {
        try {
            server = new ServerWireImpl(PORT - 4, sessionId);
            link = new StreamLink(HOST, PORT - 4);
            client = new WireImpl(link);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
        link.pullMessage(link.messageNumber(), 0, null);
        var sender = new Sender();
        sender.start();
        var clientResultSetWire = client.createResultSetWire();
        clientResultSetWire.connect(NAME);
        assertThrows(ResultSetWire.IntentioanalResultSetCloseNotification.class, () -> {
            clientResultSetWire.close();
        });
        sender.join();  // pass if join() returns
        assertEquals(COUNT, writeCount);
        assertTrue(server.isReceiveResultSetByeOk());
    }

    @Test
    void readNoRecordsTest() throws Exception {
        try {
            server = new ServerWireImpl(PORT - 5, sessionId);
            link = new StreamLink(HOST, PORT - 5);
            client = new WireImpl(link);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
        link.pullMessage(link.messageNumber(), 0, null);
        var sender = new Sender();
        sender.start();

        var clientResultSetWire = client.createResultSetWire();
        clientResultSetWire.connect(NAME);
        var recordStream = clientResultSetWire.getByteBufferBackedInput();

        assertThrows(ResultSetWire.IntentioanalResultSetCloseNotification.class, () -> {
            recordStream.close();
        });

        sender.finish();
        sender.join();
        assertTrue(server.isReceiveResultSetByeOk());
    }
}
