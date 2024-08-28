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
package com.tsurugidb.tsubakuro.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class BasicPlanNodeTest {

    @Test
    void properties() {
        var node = new BasicPlanNode("k", "t", Map.of("a", "A", "b", "B", "c", "C"));
        assertEquals("k", node.getKind());
        assertEquals("t", node.getTitle());
        assertEquals(Map.of("a", "A", "b", "B", "c", "C"), node.getAttributes());
    }

    @Test
    void addUpstream() {
        var a = node("a");
        var b = node("b");

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());

        assertEquals(Set.of(), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());

        b.addUpstream(a);

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(a), b.getUpstreams());

        assertEquals(Set.of(b), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());
    }

    @Test
    void addDownstream() {
        var a = node("a");
        var b = node("b");

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());

        assertEquals(Set.of(), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());

        a.addDownstream(b);

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(a), b.getUpstreams());

        assertEquals(Set.of(b), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());
    }

    @Test
    void orphan() {
        // [a]
        var a = node("a");
        a.bypass();

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), a.getDownstreams());
    }

    @Test
    void top() {
        // [a] - b
        var a = node("a");
        var b = node("b");
        a.addDownstream(b);

        a.bypass();

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());

        assertEquals(Set.of(), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());
    }

    @Test
    void top_multiple() {
        //      /- b
        // [a] --- c
        //      \- d
        var a = node("a");
        var b = node("b");
        var c = node("c");
        var d = node("d");
        a.addDownstream(b);
        a.addDownstream(b);
        a.addDownstream(c);
        a.addDownstream(d);
        a.bypass();

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());
        assertEquals(Set.of(), c.getUpstreams());
        assertEquals(Set.of(), d.getUpstreams());

        assertEquals(Set.of(), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());
        assertEquals(Set.of(), c.getDownstreams());
        assertEquals(Set.of(), d.getDownstreams());
    }

    @Test
    void bottom() {
        // a - [b]
        var a = node("a");
        var b = node("b");
        a.addDownstream(b);

        b.bypass();

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());

        assertEquals(Set.of(), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());
    }

    @Test
    void bottom_multiple() {
        // a -\
        // b -- [d]
        // c -/
        var a = node("a");
        var b = node("b");
        var c = node("c");
        var d = node("d");
        a.addDownstream(d);
        b.addDownstream(d);
        c.addDownstream(d);

        d.bypass();

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());
        assertEquals(Set.of(), c.getUpstreams());
        assertEquals(Set.of(), d.getUpstreams());

        assertEquals(Set.of(), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());
        assertEquals(Set.of(), c.getDownstreams());
        assertEquals(Set.of(), d.getDownstreams());
    }

    @Test
    void middle() {
        // a - [b] - c
        var a = node("a");
        var b = node("b");
        var c = node("c");
        a.addDownstream(b);
        b.addDownstream(c);

        b.bypass();

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());
        assertEquals(Set.of(a), c.getUpstreams());

        assertEquals(Set.of(c), a.getDownstreams());
        assertEquals(Set.of(), b.getDownstreams());
        assertEquals(Set.of(), c.getDownstreams());
    }

    @Test
    void middle_multiple() {
        // a -\     /- e
        // b -- [d] -- f
        // c -/     \- g
        var a = node("a");
        var b = node("b");
        var c = node("c");
        var d = node("d");
        var e = node("e");
        var f = node("f");
        var g = node("g");
        a.addDownstream(d);
        b.addDownstream(d);
        c.addDownstream(d);
        d.addDownstream(e);
        d.addDownstream(f);
        d.addDownstream(g);

        d.bypass();

        assertEquals(Set.of(), a.getUpstreams());
        assertEquals(Set.of(), b.getUpstreams());
        assertEquals(Set.of(), c.getUpstreams());
        assertEquals(Set.of(), d.getUpstreams());
        assertEquals(Set.of(a, b, c), e.getUpstreams());
        assertEquals(Set.of(a, b, c), f.getUpstreams());
        assertEquals(Set.of(a, b, c), g.getUpstreams());

        assertEquals(Set.of(e, f, g), a.getDownstreams());
        assertEquals(Set.of(e, f, g), b.getDownstreams());
        assertEquals(Set.of(e, f, g), c.getDownstreams());
        assertEquals(Set.of(), d.getDownstreams());
        assertEquals(Set.of(), e.getDownstreams());
        assertEquals(Set.of(), f.getDownstreams());
        assertEquals(Set.of(), g.getDownstreams());
    }

    private static BasicPlanNode node(String kind) {
        return new BasicPlanNode(kind, kind, Map.of());
    }
}
