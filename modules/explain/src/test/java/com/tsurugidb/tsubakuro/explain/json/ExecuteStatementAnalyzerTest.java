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
