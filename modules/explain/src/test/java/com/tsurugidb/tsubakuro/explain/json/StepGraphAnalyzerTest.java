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

import static com.tsurugidb.tsubakuro.explain.json.TestUtil.all;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.bottom;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.get;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.kind;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.next;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.top;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

class StepGraphAnalyzerTest {

    private static PlanGraph load(String name) throws IOException, PlanGraphException {
        return load(name, TestUtil.TRUE);
    }

    private static PlanGraph load(String name, Predicate<? super PlanNode> filter)
            throws IOException, PlanGraphException {
        var json = TestUtil.loadJson(String.format("plan.files/%s.json", name));
        var analyzer = new StepGraphAnalyzer(Map.of(), TestUtil.DEFAULT_EDGE_EXTRACTORS, filter);
        var graph = analyzer.analyze((ArrayNode) json);
        return graph;
    }

    @Test
    void simple() throws Exception {
        /*
         * [values:ri - emit:ro]
         */
        var graph = load("values");
        var ri = get(graph, "values");
        top(ri);
        var ro = next(ri);
        kind(ro, "emit");
        bottom(ro);
    }

    @Test
    void filter() throws Exception {
        /*
         * [scan:r0 - filter:r1 - emit:ro]
         */
        var graph = load("filter");
        var r0 = get(graph, "scan");
        top(r0);

        var r1 = next(r0);
        kind(r1, "filter");

        var r2 = next(r1);
        kind(r2, "emit");
        bottom(r2);
    }

    @Test
    void filter_skip() throws Exception {
        /*
         * [scan:r0 - filter:! - emit:ro]
         */
        var graph = load("filter", n -> !n.getKind().equals("filter"));
        var r0 = get(graph, "scan");
        top(r0);

        var r2 = next(r0);
        kind(r2, "emit");
        bottom(r2);
    }

    @Test
    void buffer() throws Exception {
        /*
         * [scan:r0 - buffer:r1 - emit:ro0]
         *                  \
         *                   emit:ro1]
         */
        var graph = load("buffer");
        var r0 = get(graph, "scan");
        top(r0);
        var r1 = next(r0);
        kind(r1, "buffer");
        assertEquals(2, r1.getDownstreams().size());
        for (var ro : r1.getDownstreams()) {
            kind(ro, "emit");
            bottom(ro);
        }
    }

    @Test
    void forward() throws Exception {
        /*
         * [scan:r0 - offer:r1]:p0 - [forward]:e0 - [take_flat:r2 - emit:r3]:p1
         */
        var graph = load("forward");

        var r0 = get(graph, "scan");
        top(r0);

        var r1 = next(r0);
        kind(r1, "offer");

        var e0 = next(r1);
        kind(e0, "forward_exchange");

        var r2 = next(e0);
        kind(r2, "take_flat");

        var r3 = next(r2);
        kind(r3, "emit");
        bottom(r3);
    }

    @Test
    void group() throws Exception {
        /*
         * [scan:r0 - offer:r1]:p0 - [group]:e0 - [take_group:r2 - flatten_group:r3 - emit:r4]:p1
         */
        var graph = load("group");

        var r0 = get(graph, "scan");
        top(r0);

        var r1 = next(r0);
        kind(r1, "offer");

        var e0 = next(r1);
        kind(e0, "group_exchange");

        var r2 = next(e0);
        kind(r2, "take_group");

        var r3 = next(r2);
        kind(r3, "flatten_group");

        var r4 = next(r3);
        kind(r4, "emit");
        bottom(r4);
    }

    @Test
    void aggregate() throws Exception {
        /*
         * [scan:r0 - offer:r1]:p0 - [aggregate]:e0 - [take_group:r2 - flatten_group:r3 - emit:r4]:p1
         */
        var graph = load("aggregate");

        var r0 = get(graph, "scan");
        top(r0);

        var r1 = next(r0);
        kind(r1, "offer");

        var e0 = next(r1);
        kind(e0, "aggregate_exchange");

        var r2 = next(e0);
        kind(r2, "take_group");

        var r3 = next(r2);
        kind(r3, "flatten_group");

        var r4 = next(r3);
        kind(r4, "emit");
        bottom(r4);
    }

    @Test
    void join_group() throws Exception {
        /*
         * [scan:rl0 - offer:rl1]:pl - [group]:el -\
         *                                         [take_cogroup:rj0 - join_group:rj1 - emit:rjo]:pj
         * [scan:rr0 - offer:rr1]:pr - [group]:er -/
         */
        var graph = load("join_group");

        var rj0 = get(graph, "take_cogroup");

        var r0s = all(graph, "scan");
        assertEquals(2, r0s.size());
        for (var r0 : r0s) {
            var r1 = next(r0);
            kind(r1, "offer");

            var e = next(r1);
            kind(e, "group_exchange");

            var rj = next(e);
            assertSame(rj0, rj);
        }

        assertEquals(2, rj0.getUpstreams().size());

        var rj1 = next(rj0);
        kind(rj1, "join_group");

        var rjo = next(rj1);
        kind(rjo, "emit");
        bottom(rjo);
    }

    @Test
    void join_find_index() throws Exception {
        /*
         * [scan:r0 - join_find:r1 - emit:ro]:p0
         */
        var graph = load("join_find-index");

        var r0 = get(graph, "scan");
        top(r0);

        var r1 = next(r0);
        kind(r1, "join_find");

        var r2 = next(r1);
        kind(r2, "emit");
        bottom(r2);
    }

    @Test
    void join_scan_index() throws Exception {
        /*
         * [scan:r0 - join_scan:r1 - emit:ro]:p0
         */
        var graph = load("join_scan-index");

        var r0 = get(graph, "scan");
        top(r0);

        var r1 = next(r0);
        kind(r1, "join_scan");

        var r2 = next(r1);
        kind(r2, "emit");
        bottom(r2);
    }

    @Test
    void join_find_broadcast() throws Exception {
        /*
         *                                 [scan:r0 - join_find:r1 - emit:ro]:p0
         *                                            /
         * [scan:r2 - offer:r3]:p1 - [broadcast]:e0 -/
         */
        var graph = load("join_find-broadcast");

        var r1 = get(graph, "join_find");
        assertEquals(2, r1.getUpstreams().size());

        var ss = all(graph, "scan");
        assertEquals(2, ss.size());
        for (var s : ss) {
            top(s);

            var r2 = next(s);
            if (r2 != r1) {
                kind(r2, "offer");

                var e0 = next(r2);
                kind(e0, "broadcast_exchange");
                assertSame(r1, next(e0));
            }
        }

        var ro = next(r1);
        kind(ro, "emit");
        bottom(ro);
    }

    @Test
    void join_scan_broadcast() throws Exception {
        /*
         *                                 [scan:r0 - join_scan:r1 - emit:ro]:p0
         *                                            /
         * [scan:r2 - offer:r3]:p1 - [broadcast]:e0 -/
         */
        var graph = load("join_scan-broadcast");

        var r1 = get(graph, "join_scan");
        assertEquals(2, r1.getUpstreams().size());

        var ss = all(graph, "scan");
        assertEquals(2, ss.size());
        for (var s : ss) {
            top(s);

            var r2 = next(s);
            if (r2 != r1) {
                kind(r2, "offer");

                var e0 = next(r2);
                kind(e0, "broadcast_exchange");
                assertSame(r1, next(e0));
            }
        }

        var ro = next(r1);
        kind(ro, "emit");
        bottom(ro);
    }

    @Test
    void forward_chain() throws Exception {
        /*
         * [scan:r0 - offer:r1]:p0 - [forward]:e0 - [take_flat:r2- offer:r3]:p1 - [forward]:e1 - [take_flat:r4 - emit:r5]:p2
         */
        var graph = load("forward-chain");

        var r0 = get(graph, "scan");
        top(r0);

        var r1 = next(r0);
        kind(r1, "offer");

        var e0 = next(r1);
        kind(e0, "forward_exchange");

        var r2 = next(e0);
        kind(r2, "take_flat");

        var r3 = next(r2);
        kind(r3, "offer");

        var e1 = next(r3);
        kind(e1, "forward_exchange");

        var r4 = next(e1);
        kind(r4, "take_flat");

        var r5 = next(r4);
        kind(r5, "emit");
        bottom(r5);
    }
}
