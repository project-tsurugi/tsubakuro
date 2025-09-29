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

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class BasicDocumentSnippetTest {

    private static Doc pick(String name) {
        try {
            var doc = BasicDocumentSnippetTest.class.getDeclaredMethod(name).getAnnotation(Doc.class);
            assertNotNull(doc);
            return doc;
        } catch (NoSuchMethodException | SecurityException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void new_empty() {
        var doc = new BasicDocumentSnippet();
        assertEquals(new BasicDocumentSnippet(List.of(), List.of(), List.of(), null), doc);
    }

    @Doc("OK")
    @Test
    void of_simple() {
        var a = pick("of_simple");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(BasicDocumentSnippet.of("OK"), doc);
    }

    @Doc({"a", "b", "c"})
    @Test
    void of_description_multiple() {
        var a = pick("of_description_multiple");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(BasicDocumentSnippet.of("a", "b", "c"), doc);
    }

    @Doc(value = "OK", note = "NOTE")
    @Test
    void of_note() {
        var a = pick("of_note");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(new BasicDocumentSnippet(List.of("OK"), List.of("NOTE"), List.of()), doc);
    }

    @Doc(value = "OK", note = {"a", "b", "c"})
    @Test
    void of_note_multiple() {
        var a = pick("of_note_multiple");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(new BasicDocumentSnippet(List.of("OK"), List.of("a", "b", "c"), List.of()), doc);
    }

    @Doc(value = "OK", reference = "http://example.com")
    @Test
    void of_reference() {
        var a = pick("of_reference");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(
                new BasicDocumentSnippet(
                        List.of("OK"),
                        List.of(),
                        List.of(new BasicDocumentSnippet.BasicReference("http://example.com", null))),
                doc,
                doc.toString());
    }

    @Doc(value = "OK", reference = " example page @ http://example.com")
    @Test
    void of_reference_title() {
        var a = pick("of_reference_title");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(
                new BasicDocumentSnippet(
                        List.of("OK"),
                        List.of(),
                        List.of(new BasicDocumentSnippet.BasicReference("http://example.com", "example page"))),
                doc,
                doc.toString());
    }

    @Doc(value = "OK", reference = " page @ example @ http://example.com")
    @Test
    void of_reference_title_multiple_delimiter() {
        var a = pick("of_reference_title_multiple_delimiter");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(
                new BasicDocumentSnippet(
                        List.of("OK"),
                        List.of(),
                        List.of(new BasicDocumentSnippet.BasicReference("http://example.com", "page @ example"))),
                doc,
                doc.toString());
    }

    @Doc(value = "OK", reference = {"http://example.com/1", "http://example.com/2", "http://example.com/3"})
    @Test
    void of_reference_multiple() {
        var a = pick("of_reference_multiple");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(
                new BasicDocumentSnippet(
                        List.of("OK"),
                        List.of(),
                        List.of(
                                new BasicDocumentSnippet.BasicReference("http://example.com/1", null),
                                new BasicDocumentSnippet.BasicReference("http://example.com/2", null),
                                new BasicDocumentSnippet.BasicReference("http://example.com/3", null))),
                doc,
                doc.toString());
    }

    @Doc(value = "OK", code = 123)
    @Test
    void of_code() {
        var a = pick("of_code");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(
                new BasicDocumentSnippet(
                        List.of("OK"),
                        List.of(),
                        List.of(),
                        123),
                doc,
                doc.toString());
        assertEquals(OptionalInt.of(123), doc.getCode());
    }

    @Doc(value = "OK", code = Doc.CODE_UNSPECIFIED)
    @Test
    void of_code_unspecified() {
        var a = pick("of_code_unspecified");
        var doc = BasicDocumentSnippet.of(a);
        assertEquals(
                new BasicDocumentSnippet(
                        List.of("OK"),
                        List.of(),
                        List.of()),
                doc,
                doc.toString());
        assertEquals(OptionalInt.empty(), doc.getCode());
    }
}
