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
