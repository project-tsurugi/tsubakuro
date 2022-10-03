package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class ScanPropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("scan", new ScanPropertyExtractor()));
        return TestUtil.get(plan, "scan");
    }

    @Test
    void table() throws Exception {
        var node = load("scan");
        assertEquals("scan", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("table", attrs.get("source"));
        assertEquals("full-scan", attrs.get("access"));
        assertEquals("T0", attrs.get("table"));
        assertNull(attrs.get("index"));
    }

    @Test
    void index() throws Exception {
        var node = load("scan-secondary");
        assertEquals("scan", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("index", attrs.get("source"));
        assertEquals("full-scan", attrs.get("access"));
        assertEquals("T0", attrs.get("table"));
        assertEquals("I0S", attrs.get("index"));
    }

    @Test
    void range() throws Exception {
        var node = load("scan-range");
        assertEquals("scan", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("table", attrs.get("source"));
        assertEquals("range-scan", attrs.get("access"));
        assertEquals("T0", attrs.get("table"));
        assertNull(attrs.get("index"));
    }
}
