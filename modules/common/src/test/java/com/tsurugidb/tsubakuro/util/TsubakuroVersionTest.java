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
package com.tsurugidb.tsubakuro.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.jupiter.api.Test;

class TsubakuroVersionTest {

    @Test
    void getBuildTimestamp() throws IOException {
        String version = TsubakuroVersion.getBuildTimestamp("version-test");
        assertEquals("2023-10-24T12:52:36.203+0900", version);
    }

    @Test
    void getBuildRevision() throws IOException {
        String version = TsubakuroVersion.getBuildRevision("version-test");
        assertEquals("abcdefg", version);
    }

    @Test
    void getBuildVersion() throws IOException {
        String version = TsubakuroVersion.getBuildVersion("version-test");
        assertEquals("1.0.0-TEST", version);
    }

    @Test
    void notFoundVersionFile() {
        var e = assertThrows(IOException.class, () -> {
            TsubakuroVersion.getProperties("not-found");
        });
        assertEquals("version file load error. module=tsubakuro-not-found", e.getMessage());
        var c = (FileNotFoundException) e.getCause();
        assertEquals("missing version file. path=/META-INF/tsurugidb/tsubakuro-not-found.properties", c.getMessage());
    }
}
