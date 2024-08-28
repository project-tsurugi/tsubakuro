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
package com.tsurugidb.tsubakuro.channel.stream.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.ConnectorFactory;

class StreamConnectorFactoryTest {

    @Test
    void spi() {
        assertTrue(ServiceLoader.load(ConnectorFactory.class)
                .stream()
                .anyMatch(it -> it.type() == StreamConnectorFactory.class));
    }

    @Test
    void testTryCreate() {
        var factory = new StreamConnectorFactory();

        assertTrue(factory.tryCreate(URI.create("tcp://localhost:8081")).isPresent());
        assertFalse(factory.tryCreate(URI.create("tcp://localhost")).isPresent());

        // other than host:port parts will be ignored
        assertTrue(factory.tryCreate(URI.create("tcp://localhost:8081/")).isPresent());
        assertTrue(factory.tryCreate(URI.create("tcp://localhost:8081/path/to/something?query")).isPresent());
        assertTrue(factory.tryCreate(URI.create("tcp://user:pass@localhost:8081")).isPresent());

        assertFalse(factory.tryCreate(URI.create("udp://localhost:8081")).isPresent());
        assertFalse(factory.tryCreate(URI.create("http://www.example.com/")).isPresent());
        assertFalse(factory.tryCreate(URI.create("ipc:tsurugi")).isPresent());
    }
}
