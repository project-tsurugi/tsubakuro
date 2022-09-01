package  com.tsurugidb.tsubakuro.channel.common.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class ConnectorTest {

    @Test
    void create() {
        var connector = Connector.create("testing:example");
        assertTrue(connector instanceof TestingConnector);

        var tc = (TestingConnector) connector;
        assertEquals(URI.create("testing:example"), tc.endpoint);

        assertThrows(IllegalArgumentException.class, () -> Connector.create(" "));
        assertThrows(NoSuchElementException.class, () -> Connector.create("invalid:invalid"));
    }
}
