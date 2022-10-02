package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class JoinFindPropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("join_find", new JoinFindPropertyExtractor()));
        return TestUtil.get(plan, "join_find");
    }

    @Test
    void table() throws Exception {
        var node = load("join_find-index");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("inner", attrs.get("join-type"));
        assertEquals("table", attrs.get("source"));
        assertEquals("point", attrs.get("access"));
        assertEquals("T1", attrs.get("table"));
        assertNull(attrs.get("index"));
    }

    @Test
    void index() throws Exception {
        var node = load("join_find-index-secondary");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("left_outer", attrs.get("join-type"));
        assertEquals("index", attrs.get("source"));
        assertEquals("point", attrs.get("access"));
        assertEquals("T1", attrs.get("table"));
        assertEquals("I1S", attrs.get("index"));
    }

    @Test
    void broadcast() throws Exception {
        var node = load("join_find-broadcast");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("inner", attrs.get("join-type"));
        assertEquals("broadcast", attrs.get("source"));
        assertEquals("point", attrs.get("access"));
        assertNull(attrs.get("table"));
        assertNull(attrs.get("index"));
    }
}
