package com.tsurugidb.tsubakuro.channel.stream;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamWireReceiver extends Thread {
    private StreamWire streamWire;

    static final Logger LOG = LoggerFactory.getLogger(StreamWireReceiver.class);

    public StreamWireReceiver(StreamWire streamWire) {
        this.streamWire = streamWire;
    }

    public void run() {
        try {
            while (true) {
                if (!streamWire.pull()) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
