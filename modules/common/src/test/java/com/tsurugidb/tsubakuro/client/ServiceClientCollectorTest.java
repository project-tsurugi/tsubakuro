package com.tsurugidb.tsubakuro.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ServiceClientCollectorTest {

    private Path temp;

    private URLClassLoader prepare(String... lines) throws IOException {
        assertNull(temp);
        temp = Files.createTempDirectory("tsubakuro-");
        var file = temp.resolve(Path.of(ServiceClientCollector.PATH_METADATA));
        Files.createDirectories(file.getParent());
        Files.createFile(file);
        Files.writeString(
                file,
                String.join("\n", lines),
                ServiceClientCollector.ENCODING_METADATA);
        return new URLClassLoader(new URL[] { temp.toFile().toURI().toURL() });
    }

    @AfterEach
    void teardown() throws IOException {
        if (temp != null) {
            Files.walkFileTree(temp, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return super.postVisitDirectory(dir, exc);
                }
            });
        }
    }

    @Test
    void collect() throws Exception {
        try (var cl = prepare(new String[] {
                MockClient.class.getName(),
        })) {
            var results = ServiceClientCollector.collect(cl, true);
            assertEquals(List.of(MockClient.class), results);
        }
    }

    @Test
    void comments() throws Exception {
        try (var cl = prepare(new String[] {
                "# begin",
                "",
                "  " + MockClient.class.getName() + " # testing",
                "# end",
        })) {
            var results = ServiceClientCollector.collect(cl, true);
            assertEquals(List.of(MockClient.class), results);
        }
    }

    @Test
    void empty() throws Exception {
        try (var cl = prepare(new String[] {})) {
            var results = ServiceClientCollector.collect(cl, true);
            assertEquals(List.of(), results);
        }
    }

    @Test
    void missing() throws Exception {
        try (var cl = prepare(new String[] {
                MockClient.class.getName() + "_MISSING",
        })) {
            assertThrows(IOException.class, () -> ServiceClientCollector.collect(cl, true));
        }
    }

    @Test
    void inconsistent() throws Exception {
        try (var cl = prepare(new String[] {
                ServiceClientCollector.class.getName(),
        })) {
            assertThrows(IOException.class, () -> ServiceClientCollector.collect(cl, true));
        }
    }

    @Test
    void collect_ignoreError() throws Exception {
        try (var cl = prepare(new String[] {
                ServiceClientCollector.class.getName(),
                MockClient.class.getName(),
                MockClient.class.getName() + "_MISSING",
        })) {
            var results = ServiceClientCollector.collect(cl, false);
            assertEquals(List.of(MockClient.class), results);
        }
    }
}
