package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class WritePropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("write", new WritePropertyExtractor()));
        return TestUtil.get(plan, "write");
    }

    @Test
    void update() throws Exception {
        var node = load("write");
        assertEquals("write", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("update", attrs.get("write-kind"));
        assertEquals("T1", attrs.get("table"));
    }
}
