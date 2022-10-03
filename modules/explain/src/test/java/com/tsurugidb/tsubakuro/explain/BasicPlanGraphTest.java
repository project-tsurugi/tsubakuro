package com.tsurugidb.tsubakuro.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class BasicPlanGraphTest {

    @Test
    void collect() {
        var a = node("a");
        var b = node("b");
        var c = node("c");

        a.addDownstream(b);
        b.addDownstream(c);

        var graph = new BasicPlanGraph(Set.of(a));
        assertEquals(Set.of(a, b, c), graph.getNodes());
    }

    @Test
    void sources() {
        // a -\   /- d
        // b -- c -- e
        var a = node("a");
        var b = node("b");
        var c = node("c");
        var d = node("d");
        var e = node("e");

        c.addUpstream(a);
        c.addUpstream(b);
        c.addDownstream(d);
        c.addDownstream(e);

        var graph = new BasicPlanGraph(Set.of(a));
        assertEquals(Set.of(a, b), graph.getSources());
    }

    @Test
    void destinations() {
        // a -\   /- d
        // b -- c -- e
        var a = node("a");
        var b = node("b");
        var c = node("c");
        var d = node("d");
        var e = node("e");

        c.addUpstream(a);
        c.addUpstream(b);
        c.addDownstream(d);
        c.addDownstream(e);

        var graph = new BasicPlanGraph(Set.of(e));
        assertEquals(Set.of(d, e), graph.getDestinations());
    }

    private static BasicPlanNode node(String kind) {
        return new BasicPlanNode(kind, kind, Map.of());
    }
}
