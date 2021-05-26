package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

import org.junit.jupiter.api.Test;

class WireImplTest {
    WireImpl client;
    ServerWireImpl server;

    @Test
    void simple() {
	try {
	    server = new ServerWireImpl("tsubakuro-session1");
	    client = new WireImpl("tsubakuro-session1");
	} catch (IOException e) {
	    fail("cought IOException");
	}

	assertAll(
		  () -> assertEquals(1, 1),
		  () -> assertEquals(2.0, 2.0));
    }

}
