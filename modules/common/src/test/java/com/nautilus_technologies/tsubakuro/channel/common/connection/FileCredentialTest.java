package com.nautilus_technologies.tsubakuro.channel.common.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileCredentialTest {

    @Test
    void restore(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("tmp.json");

        var creds = new FileCredential("A", "B");
        creds.dump(file);

        var restored = FileCredential.load(file);
        assertEquals(restored.getEncryptedName(), "A");
        assertEquals(restored.getEncryptedPassword(), "B");
    }

    @Test
    void loadMissing(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("tmp.json");
        assertThrows(FileNotFoundException.class, () -> FileCredential.load(file));
    }
}
