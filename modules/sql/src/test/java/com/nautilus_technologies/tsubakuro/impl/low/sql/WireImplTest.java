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

    @Test
    void simple() {
        assertAll(
		  () -> assertEquals(1, 1),
		  () -> assertEquals(2.0, 2.0));
    }
}
