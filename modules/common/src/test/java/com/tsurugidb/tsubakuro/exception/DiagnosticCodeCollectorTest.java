package com.tsurugidb.tsubakuro.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.util.BasicDocumentSnippet;

class DiagnosticCodeCollectorTest {

    private Path temp;

    private URLClassLoader prepare(String... lines) throws IOException {
        assertNull(temp);
        temp = Files.createTempDirectory("tsubakuro-");
        var file = temp.resolve(Path.of(DiagnosticCodeCollector.PATH_METADATA));
        Files.createDirectories(file.getParent());
        Files.createFile(file);
        Files.writeString(
                file,
                String.join("\n", lines),
                DiagnosticCodeCollector.ENCODING_METADATA);
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
                MockDiagnosticCode.class.getName(),
        })) {
            var results = DiagnosticCodeCollector.collect(cl, true);
            assertTrue(results.contains(MockDiagnosticCode.class));
        }
    }

    @Test
    void comments() throws Exception {
        try (var cl = prepare(new String[] {
                "# begin",
                "",
                "  " + MockDiagnosticCode.class.getName() + " # testing",
                "# end",
        })) {
            var results = DiagnosticCodeCollector.collect(cl, true);
            assertTrue(results.contains(MockDiagnosticCode.class));
        }
    }

    @Test
    void empty() throws Exception {
        try (var cl = prepare(new String[] {})) {
            var results = DiagnosticCodeCollector.collect(cl, true);
            assertFalse(results.contains(MockDiagnosticCode.class));
        }
    }

    @Test
    void missing() throws Exception {
        try (var cl = prepare(new String[] {
                MockDiagnosticCode.class.getName() + "_MISSING",
        })) {
            assertThrows(IOException.class, () -> DiagnosticCodeCollector.collect(cl, true));
        }
    }

    @Test
    void inconsistent() throws Exception {
        try (var cl = prepare(new String[] {
                DiagnosticCodeCollector.class.getName(),
        })) {
            assertThrows(IOException.class, () -> DiagnosticCodeCollector.collect(cl, true));
        }
    }

    @Test
    void collect_ignoreError() throws Exception {
        try (var cl = prepare(new String[] {
                DiagnosticCodeCollector.class.getName(),
                MockDiagnosticCode.class.getName(),
                MockDiagnosticCode.class.getName() + "_MISSING",
        })) {
            var results = DiagnosticCodeCollector.collect(cl, false);
            assertTrue(results.contains(MockDiagnosticCode.class));
        }
    }

    @Test
    void collect_system() throws Exception {
        var results = DiagnosticCodeCollector.collect(true);
        assertTrue(results.contains(CoreServiceCode.class));
    }

    @Test
    void findDocument() {
        var docs = DiagnosticCodeCollector.findDocument(MockDiagnosticCode.class);
        assertEquals(1, docs.size());

        var c = docs.get(0);
        assertEquals(MockDiagnosticCode.TESTING, c.getElement());
        assertEquals(BasicDocumentSnippet.of("OK"), c.getDocument());
    }

    @Test
    void findDocument_system() {
        var docs = DiagnosticCodeCollector.findDocument(CoreServiceCode.class);
        for (var element : docs) {
            System.out.println(element);
        }
    }
}
