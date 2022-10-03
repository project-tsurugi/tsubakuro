package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

class ConstantPropertyExtractorTest {

    @Test
    void intersection_group() throws Exception {
        var plan = TestUtil.loadPlan(
                "intersection_group",
                Map.of("intersection_group", new ConstantPropertyExtractor("intersection", Map.of())));

        var node = TestUtil.get(plan, "intersection_group");
        assertEquals("intersection", node.getTitle());

        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void difference_group() throws Exception {
        var plan = TestUtil.loadPlan(
                "difference_group",
                Map.of("difference_group", new ConstantPropertyExtractor("difference", Map.of())));

        var node = TestUtil.get(plan, "difference_group");
        assertEquals("difference", node.getTitle());

        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void aggregate_group() throws Exception {
        var plan = TestUtil.loadPlan(
                "aggregate_group",
                Map.of("aggregate_group", new ConstantPropertyExtractor("aggregate", Map.of("incremental", "false"))));

        var node = TestUtil.get(plan, "aggregate_group");
        assertEquals("aggregate", node.getTitle());

        assertEquals(Map.of("incremental", "false"), node.getAttributes());
    }
}
