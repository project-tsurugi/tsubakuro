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
package com.tsurugidb.tsubakuro.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.json.JsonPlanGraphLoader;

class BasicPlanGraphTest {

    @Test
    void collect() {
        var a = node("a");
        var b = node("b");
        var c = node("c");

        a.addDownstream(b);
        b.addDownstream(c);

        var graph = new BasicPlanGraph(Set.of(a));
        assertEquals(Set.of(a, b, c), graph.getNodes());
    }

    @Test
    void sources() {
        // a -\   /- d
        // b -- c -- e
        var a = node("a");
        var b = node("b");
        var c = node("c");
        var d = node("d");
        var e = node("e");

        c.addUpstream(a);
        c.addUpstream(b);
        c.addDownstream(d);
        c.addDownstream(e);

        var graph = new BasicPlanGraph(Set.of(a));
        assertEquals(Set.of(a, b), graph.getSources());
    }

    @Test
    void destinations() {
        // a -\   /- d
        // b -- c -- e
        var a = node("a");
        var b = node("b");
        var c = node("c");
        var d = node("d");
        var e = node("e");

        c.addUpstream(a);
        c.addUpstream(b);
        c.addDownstream(d);
        c.addDownstream(e);

        var graph = new BasicPlanGraph(Set.of(e));
        assertEquals(Set.of(d, e), graph.getDestinations());
    }

    @Test
    void preserveOrder() throws PlanGraphException {
        var json = "{\"kind\":\"execute\",\"execution_plan\":[{\"kind\":\"process\",\"this\":\"@1\",\"operators\":[{\"kind\":\"find\",\"this\":\"@2\",\"source\":{\"kind\":\"relation\",\"binding\":{\"kind\":\"index\",\"table\":\"test\",\"simple_name\":\"test\",\"keys\":[{\"column\":\"foo\",\"direction\":\"ascendant\"}],\"values\":[\"bar\",\"zzz\"],\"features\":[\"primary\",\"find\",\"scan\",\"unique\"]}},\"columns\":[{\"source\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"table_column\",\"simple_name\":\"foo\",\"type\":{\"kind\":\"int4\"},\"criteria\":{\"nullity\":\"nullable\"},\"default_value\":{\"kind\":\"nothing\"},\"owner\":{\"kind\":\"table\",\"simple_name\":\"foo\"}},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"foo\",\"owner\":\"test\",\"type\":{\"kind\":\"int4\"}}},\"destination\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"stream_variable\",\"this\":\"@3\",\"label\":\"test::foo\"},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"foo\",\"owner\":\"test\",\"type\":{\"kind\":\"int4\"}}}},{\"source\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"table_column\",\"simple_name\":\"bar\",\"type\":{\"kind\":\"int8\"},\"criteria\":{\"nullity\":\"nullable\"},\"default_value\":{\"kind\":\"nothing\"},\"owner\":{\"kind\":\"table\",\"simple_name\":\"bar\"}},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"bar\",\"owner\":\"test\",\"type\":{\"kind\":\"int8\"}}},\"destination\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"stream_variable\",\"this\":\"@4\",\"label\":\"test::bar\"},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"bar\",\"owner\":\"test\",\"type\":{\"kind\":\"int8\"}}}},{\"source\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"table_column\",\"simple_name\":\"zzz\",\"type\":{\"kind\":\"character\",\"varying\":true,\"length\":10},\"criteria\":{\"nullity\":\"nullable\"},\"default_value\":{\"kind\":\"nothing\"},\"owner\":{\"kind\":\"table\",\"simple_name\":\"zzz\"}},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"zzz\",\"owner\":\"test\",\"type\":{\"kind\":\"character\",\"varying\":true,\"length\":10}}},\"destination\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"stream_variable\",\"this\":\"@5\",\"label\":\"test::zzz\"},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"zzz\",\"owner\":\"test\",\"type\":{\"kind\":\"character\",\"varying\":true,\"length\":10}}}}],\"keys\":[{\"variable\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"table_column\",\"simple_name\":\"foo\",\"type\":{\"kind\":\"int4\"},\"criteria\":{\"nullity\":\"nullable\"},\"default_value\":{\"kind\":\"nothing\"},\"owner\":{\"kind\":\"table\",\"simple_name\":\"foo\"}},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"foo\",\"owner\":\"test\",\"type\":{\"kind\":\"int4\"}}},\"value\":{\"kind\":\"variable_reference\",\"this\":\"@6\",\"variable\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"external_variable\",\"name\":\"foo\",\"type\":{\"kind\":\"int4\"},\"criteria\":{\"nullity\":\"nullable\"}},\"resolution\":{\"kind\":\"external\",\"name\":\"foo\",\"type\":{\"kind\":\"int4\"}}},\"resolution\":{\"type\":{\"kind\":\"int4\"}}}}],\"input_ports\":[],\"output_ports\":[{\"index\":0,\"opposite\":{\"kind\":\"emit\",\"this\":\"@7\",\"index\":0}}]},{\"kind\":\"emit\",\"this\":\"@7\",\"columns\":[{\"source\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"stream_variable\",\"this\":\"@3\",\"label\":\"test::foo\"},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"foo\",\"owner\":\"test\",\"type\":{\"kind\":\"int4\"}}},\"name\":\"foo\"},{\"source\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"stream_variable\",\"this\":\"@4\",\"label\":\"test::bar\"},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"bar\",\"owner\":\"test\",\"type\":{\"kind\":\"int8\"}}},\"name\":\"bar\"},{\"source\":{\"kind\":\"variable\",\"binding\":{\"kind\":\"stream_variable\",\"this\":\"@5\",\"label\":\"test::zzz\"},\"resolution\":{\"kind\":\"table_column\",\"simple_name\":\"zzz\",\"owner\":\"test\",\"type\":{\"kind\":\"character\",\"varying\":true,\"length\":10}}},\"name\":\"zzz\"}],\"input_ports\":[{\"index\":0,\"opposite\":{\"kind\":\"find\",\"this\":\"@2\",\"index\":0}}],\"output_ports\":[]}],\"upstreams\":[],\"downstreams\":[]}]}";
        var loader = JsonPlanGraphLoader.newBuilder().build();
        var planGraph = loader.load(json);
        assertInstanceOf(BasicPlanGraph.class, planGraph);
        var expected = planGraph.toString();

        for (int i = 0; i < 1000; i++) {
            var actual = loader.load(json).toString();
            assertEquals(expected, actual);
        }
    }

    private static BasicPlanNode node(String kind) {
        return new BasicPlanNode(kind, kind, Map.of());
    }
}
