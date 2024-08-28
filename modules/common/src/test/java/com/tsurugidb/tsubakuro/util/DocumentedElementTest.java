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

import java.util.List;

import org.junit.jupiter.api.Test;

class DocumentedElementTest {

    enum Empty {
        // no constants
    }

    @Test
    void constantsOf_empty() {
        var constants = DocumentedElement.constantsOf(Empty.class);
        assertEquals(List.of(), constants);
    }

    enum Simple {
        @Doc("OK")
        C
    }

    @Test
    void constantsOf_simple() {
        var constants = DocumentedElement.constantsOf(Simple.class);
        assertEquals(1, constants.size());

        var c = constants.get(0);
        assertEquals(Simple.C, c.getElement());
        assertEquals(BasicDocumentSnippet.of("OK"), c.getDocument());
    }

    enum Multiple {
        @Doc("a") A,
        @Doc("b") B,
        @Doc("c") C,
    }

    @Test
    void constantsOf_multiple() {
        var constants = DocumentedElement.constantsOf(Multiple.class);
        assertEquals(3, constants.size());

        var a = constants.get(0);
        assertEquals(Multiple.A, a.getElement());
        assertEquals(BasicDocumentSnippet.of("a"), a.getDocument());

        var b = constants.get(1);
        assertEquals(Multiple.B, b.getElement());
        assertEquals(BasicDocumentSnippet.of("b"), b.getDocument());

        var c = constants.get(2);
        assertEquals(Multiple.C, c.getElement());
        assertEquals(BasicDocumentSnippet.of("c"), c.getDocument());
    }


    enum NoDoc {
        C
    }

    @Test
    void constantsOf_nodoc() {
        var constants = DocumentedElement.constantsOf(NoDoc.class);
        assertEquals(1, constants.size());

        var c = constants.get(0);
        assertEquals(NoDoc.C, c.getElement());
        assertEquals(new BasicDocumentSnippet(), c.getDocument());
    }

    @Test
    void constantsOf_not_enum() {
        var constants = DocumentedElement.constantsOf(DocumentedElement.class);
        assertEquals(List.of(), constants);
    }
}
