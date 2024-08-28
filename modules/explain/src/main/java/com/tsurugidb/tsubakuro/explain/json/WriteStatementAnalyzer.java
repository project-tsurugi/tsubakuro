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

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.BasicPlanGraph;
import com.tsurugidb.tsubakuro.explain.BasicPlanNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

/**
 * Analyzes {@code write} statement.
 */
public class WriteStatementAnalyzer implements StatementAnalyzer {

    private static final PropertyExtractor PROPERTY_EXTRACTOR = new WritePropertyExtractor();

    private static final String K_KIND = "write"; //$NON-NLS-1$

    static final Logger LOG = LoggerFactory.getLogger(WriteStatementAnalyzer.class);

    private final Predicate<? super PlanNode> nodeFilter;

    /**
     * Creates a new instance.
     * @param nodeFilter predicate to test whether keep individual operators
     */
    public WriteStatementAnalyzer(@Nonnull Predicate<? super PlanNode> nodeFilter) {
        Objects.requireNonNull(nodeFilter);
        this.nodeFilter = nodeFilter;
    }

    @Override
    public PlanGraph analyze(@Nonnull ObjectNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        JsonUtil.checkKind(node, K_KIND);
        var write = new BasicPlanNode(
                "write",
                PROPERTY_EXTRACTOR.getTitle(node).orElse("write"),
                PROPERTY_EXTRACTOR.getAttributes(node));
        var values = new BasicPlanNode("values", "values", Map.of());
        var candidates = new ArrayList<BasicPlanNode>();
        candidates.add(write);
        candidates.add(values);
        values.addDownstream(write);

        var nodes = new ArrayList<BasicPlanNode>();
        for (var element : candidates) {
            if (nodeFilter.test(element)) {
                nodes.add(element);
            } else {
                element.bypass();
            }
        }
        return new BasicPlanGraph(nodes);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
