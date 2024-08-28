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
