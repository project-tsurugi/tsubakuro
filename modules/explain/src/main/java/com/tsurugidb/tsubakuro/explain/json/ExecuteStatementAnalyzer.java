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

import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Analyzes {@code execute} statement.
 */
public class ExecuteStatementAnalyzer implements StatementAnalyzer {

    private static final String K_KIND = "execute"; //$NON-NLS-1$

    private static final String K_EXECUTION_PLAN = "execution_plan"; //$NON-NLS-1$

    static final Logger LOG = LoggerFactory.getLogger(ExecuteStatementAnalyzer.class);

    private final StepGraphAnalyzer stepGraphAnalyzer;

    /**
     * Creates a new instance.
     * @param stepGraphAnalyzer the graph analyzer
     */
    public ExecuteStatementAnalyzer(@Nonnull StepGraphAnalyzer stepGraphAnalyzer) {
        Objects.requireNonNull(stepGraphAnalyzer);
        this.stepGraphAnalyzer = stepGraphAnalyzer;
    }

    @Override
    public PlanGraph analyze(@Nonnull ObjectNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        JsonUtil.checkKind(node, K_KIND);
        var plan = JsonUtil.getArray(node, K_EXECUTION_PLAN);
        var graph = stepGraphAnalyzer.analyze(plan);
        return graph;
    }

    @Override
    public String toString() {
        return String.format("ExecuteStatementAnalyzer(%s)", stepGraphAnalyzer);
    }
}
