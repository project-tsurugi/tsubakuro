package com.nautilus_technologies.tsubakuro.channel.ipc.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import  com.nautilus_technologies.tsubakuro.channel.common.connection.ConnectorFactory;

class IpcConnectorFactoryTest {

    @Test
    void spi() {
        assertTrue(ServiceLoader.load(ConnectorFactory.class)
                .stream()
                .anyMatch(it -> it.type() == IpcConnectorFactory.class));
    }

    @Test
    void tryCreate() {
        var factory = new IpcConnectorFactory();

        assertTrue(factory.tryCreate(URI.create("ipc:example")).isPresent());
        assertTrue(factory.tryCreate(URI.create("ipc:example?query")).isPresent());

        assertFalse(factory.tryCreate(URI.create("example:ipc")).isPresent());
        assertFalse(factory.tryCreate(URI.create("tcp://localhost:8081/")).isPresent());
        assertFalse(factory.tryCreate(URI.create("http://www.example.com/")).isPresent());
    }
}
