package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class AggregateExchangePropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("aggregate_exchange", new AggregateExchangePropertyExtractor()));
        return TestUtil.get(plan, "aggregate_exchange");
    }

    @Test
    void aggregate() throws Exception {
        var node = load("aggregate");
        assertEquals("aggregate", node.getTitle());
        assertEquals(Map.of(
                "whole", "false",
                "incremental", "true"), node.getAttributes());
    }

    @Test
    void whole() throws Exception {
        var node = load("aggregate-whole");
        assertEquals("aggregate", node.getTitle());
        assertEquals(Map.of(
                "whole", "true",
                "incremental", "true"), node.getAttributes());
    }
}
