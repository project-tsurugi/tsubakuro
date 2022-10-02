package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class FindPropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("find", new FindPropertyExtractor()));
        return TestUtil.get(plan, "find");
    }

    @Test
    void table() throws Exception {
        var node = load("find");
        assertEquals("scan", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("table", attrs.get("source"));
        assertEquals("point", attrs.get("access"));
        assertEquals("T0", attrs.get("table"));
        assertNull(attrs.get("index"));
    }

    @Test
    void index() throws Exception {
        var node = load("find-secondary");
        assertEquals("scan", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("index", attrs.get("source"));
        assertEquals("point", attrs.get("access"));
        assertEquals("T0", attrs.get("table"));
        assertEquals("I0S", attrs.get("index"));
    }
}
