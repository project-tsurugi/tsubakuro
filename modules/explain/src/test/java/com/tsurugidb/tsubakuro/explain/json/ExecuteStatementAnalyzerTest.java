package com.tsurugidb.tsubakuro.explain.json;

import static com.tsurugidb.tsubakuro.explain.json.TestUtil.DEFAULT_EDGE_EXTRACTORS;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.TRUE;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.bottom;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.get;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.kind;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.next;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.top;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

class ExecuteStatementAnalyzerTest {

    private static PlanGraph load(String name) throws IOException, PlanGraphException {
        var analyzer = new ExecuteStatementAnalyzer(new StepGraphAnalyzer(Map.of(), DEFAULT_EDGE_EXTRACTORS, TRUE));
        return TestUtil.loadStatement(name, Map.of("execute_statement", analyzer));
    }

    @Test
    void emit() throws Exception {
        /*
         * scan:in -- emit:out
         */
        var graph = load("execute-emit");

        var in = get(graph, "scan");
        top(in);

        var out = next(in);
        kind(out, "emit");
        bottom(out);
    }

}
