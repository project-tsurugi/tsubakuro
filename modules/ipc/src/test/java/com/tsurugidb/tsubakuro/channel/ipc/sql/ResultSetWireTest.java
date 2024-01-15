package com.tsurugidb.tsubakuro.channel.ipc.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.ipc.IpcLink;

class ResultSetWireTest {
    private final String NAME = "resultset";
    private final int COUNT = 128;

    private WireImpl client;
    private ServerWireImpl server;
    private final String dbName = "tsubakuro";
    private final long sessionID = 1;
    private long serverResultSetWire;
    private int writeCount;

    private class Sender extends Thread {
        public void run() {
            byte[] ba = new byte[1024];
            try {
                try {
                    Thread.sleep(100);
                } catch(InterruptedException e) {
                    // do nothing
                }
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
            } catch(IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Test
    void readRecordsTest() throws Exception {
        try {
            server = new ServerWireImpl(dbName, sessionID);
            client = new WireImpl(new IpcLink(dbName + "-" + String.valueOf(sessionID)), sessionID);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
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
    }

    @Test
    void serverCrashDetectionTest() throws Exception {
        try {
            server = new ServerWireImpl(dbName, sessionID, false);
            client = new WireImpl(new IpcLink(dbName + "-" + String.valueOf(sessionID)), sessionID);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
        var sender = new Sender();
        sender.start();

        var clientResultSetWire = client.createResultSetWire();
        clientResultSetWire.connect(NAME);
        var recordStream = clientResultSetWire.getByteBufferBackedInput();
        byte[] ba = new byte[1024];

        Throwable exception = assertThrows(IOException.class, () -> {
            recordStream.read(ba);
            clientResultSetWire.close();
        });
        // FIXME: check error code instead of message
        assertEquals("Server crashed", exception.getMessage());

        sender.join();
    }

    @Test
    void closeWithRecordRemainTest() throws Exception {
        try {
            server = new ServerWireImpl(dbName, sessionID);
            client = new WireImpl(new IpcLink(dbName + "-" + String.valueOf(sessionID)), sessionID);
        } catch (Exception e) {
            fail("cought Exception");
        }

        serverResultSetWire = server.createRSL(NAME);
        var sender = new Sender();
        sender.start();
        var clientResultSetWire = client.createResultSetWire();
        clientResultSetWire.connect(NAME);
        clientResultSetWire.close();
        sender.join();  // pass if join() returns
        assertEquals(COUNT, writeCount);
    }
}
