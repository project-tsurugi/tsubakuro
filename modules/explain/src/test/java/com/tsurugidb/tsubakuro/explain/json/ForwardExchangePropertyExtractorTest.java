package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class ForwardExchangePropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("forward_exchange", new ForwardExchangePropertyExtractor()));
        return TestUtil.get(plan, "forward_exchange");
    }

    @Test
    void forward() throws Exception {
        var node = load("forward");
        assertEquals("forward", node.getTitle());
        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void limit() throws Exception {
        var node = load("forward-limit");
        assertEquals("forward", node.getTitle());
        assertEquals(Map.of("limit", "1"), node.getAttributes());
    }
}
