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
