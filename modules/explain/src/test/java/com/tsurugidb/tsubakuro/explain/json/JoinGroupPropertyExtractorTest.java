package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class JoinGroupPropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("join_group", new JoinGroupPropertyExtractor()));
        return TestUtil.get(plan, "join_group");
    }

    @Test
    void join_group() throws Exception {
        var node = load("join_group");
        assertEquals("join", node.getTitle());

        var attrs = node.getAttributes();
        assertEquals("inner", attrs.get("join-type"));
        assertEquals("flow", attrs.get("source"));
        assertEquals("merge", attrs.get("access"));
    }
}
