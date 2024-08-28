/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
