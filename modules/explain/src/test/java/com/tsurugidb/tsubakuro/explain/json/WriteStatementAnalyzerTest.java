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

import static com.tsurugidb.tsubakuro.explain.json.TestUtil.TRUE;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.bottom;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.get;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.kind;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.next;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.top;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class WriteStatementAnalyzerTest {

    private static PlanGraph load(String name, Predicate<? super PlanNode> filter) throws IOException, PlanGraphException {
        var analyzer = new WriteStatementAnalyzer(filter);
        return TestUtil.loadStatement(name, Map.of("write_statement", analyzer));
    }

    @Test
    void write() throws Exception {
        var graph = load("write", TRUE);

        var in = get(graph, "values");
        top(in);
        assertEquals("values", in.getTitle());
        assertEquals(Map.of(), in.getAttributes());

        var out = next(in);
        kind(out, "write");
        assertEquals("write", out.getTitle());
        assertEquals(Map.of(
                "write-kind", "insert",
                "table", "T0"), out.getAttributes());

        bottom(out);
    }

    @Test
    void filter() throws Exception {
        var graph = load("write", node -> !node.getKind().equals("values"));

        var op = get(graph, "write");
        top(op);
        bottom(op);
    }
}
