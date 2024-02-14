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
