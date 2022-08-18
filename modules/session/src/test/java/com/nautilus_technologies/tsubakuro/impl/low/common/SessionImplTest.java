package com.nautilus_technologies.tsubakuro.impl.low.common;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SessionImplTest {
    @Test
    void useSessionAfterClose() throws Exception {
        var session = new SessionImpl();

        try {
            session.updateExpirationTime(300, TimeUnit.MINUTES);
        } catch (Exception e) {
            fail("cought some exception");
        }
    }
}
