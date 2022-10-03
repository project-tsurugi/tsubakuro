package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class JoinScanPropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("join_scan", new JoinScanPropertyExtractor()));
        return TestUtil.get(plan, "join_scan");
    }

    @Test
    void table() throws Exception {
        var node = load("join_scan-index");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("inner", attrs.get("join-type"));
        assertEquals("table", attrs.get("source"));
        assertEquals("range-scan", attrs.get("access"));
        assertEquals("T1", attrs.get("table"));
        assertNull(attrs.get("index"));
    }

    @Test
    void index() throws Exception {
        var node = load("join_scan-index-secondary");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("left_outer", attrs.get("join-type"));
        assertEquals("index", attrs.get("source"));
        assertEquals("range-scan", attrs.get("access"));
        assertEquals("T1", attrs.get("table"));
        assertEquals("I1S", attrs.get("index"));
    }

    @Test
    void broadcast() throws Exception {
        var node = load("join_scan-broadcast");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("inner", attrs.get("join-type"));
        assertEquals("broadcast", attrs.get("source"));
        assertEquals("range-scan", attrs.get("access"));
        assertNull(attrs.get("table"));
        assertNull(attrs.get("index"));
    }

    @Test
    void full() throws Exception {
        var node = load("join_scan-index-full");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("inner", attrs.get("join-type"));
        assertEquals("table", attrs.get("source"));
        assertEquals("full-scan", attrs.get("access"));
        assertEquals("T1", attrs.get("table"));
        assertNull(attrs.get("index"));
    }
}
