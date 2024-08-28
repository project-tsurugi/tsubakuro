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
import java.util.Optional;

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

    @ServiceMessageVersion(service = "a", major = 10, minor = 20)
    static interface A extends ServiceClient {
        // nothing
    }

    static interface Miss extends ServiceClient {
        // nothing
    }

    @Test
    void findServiceMessageVersion() throws Exception {
        var r = ServiceClientCollector.findServiceMessageVersion(A.class);
        assertEquals(Optional.of("a-10.20"), r);
    }

    @Test
    void findServiceMessageVersion_missing() throws Exception {
        var r = ServiceClientCollector.findServiceMessageVersion(Miss.class);
        assertEquals(Optional.empty(), r);
    }
}
