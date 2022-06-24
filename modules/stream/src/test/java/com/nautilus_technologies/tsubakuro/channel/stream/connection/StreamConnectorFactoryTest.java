package com.nautilus_technologies.tsubakuro.channel.stream.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import  com.nautilus_technologies.tsubakuro.channel.common.connection.ConnectorFactory;

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
