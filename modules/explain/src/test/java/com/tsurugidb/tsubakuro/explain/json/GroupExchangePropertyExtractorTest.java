package com.tsurugidb.tsubakuro.explain.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class GroupExchangePropertyExtractorTest {

    private static PlanNode load(String name) throws IOException, PlanGraphException {
        var plan = TestUtil.loadPlan(name, Map.of("group_exchange", new GroupExchangePropertyExtractor()));
        return TestUtil.get(plan, "group_exchange");
    }

    @Test
    void group() throws Exception {
        var node = load("group");
        assertEquals("group", node.getTitle());
        assertEquals(Map.of(
                "whole", "false",
                "sorted", "false"), node.getAttributes());
    }

    @Test
    void whole() throws Exception {
        var node = load("group-whole");
        assertEquals("group", node.getTitle());
        assertEquals(Map.of(
                "whole", "true",
                "sorted", "false"), node.getAttributes());
    }

    @Test
    void sorted() throws Exception {
        var node = load("group-sorted");
        assertEquals("group", node.getTitle());
        assertEquals(Map.of(
                "whole", "false",
                "sorted", "true"), node.getAttributes());
    }

    @Test
    void limit() throws Exception {
        var node = load("group-limit");
        assertEquals("group", node.getTitle());
        assertEquals(Map.of(
                "whole", "false",
                "sorted", "false",
                "limit", "1"), node.getAttributes());
    }
}
