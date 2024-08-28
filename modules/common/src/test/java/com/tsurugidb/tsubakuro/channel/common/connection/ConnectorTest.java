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
