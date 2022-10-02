package com.tsurugidb.tsubakuro.explain.json;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts execution plan information from statement information.
 */
@FunctionalInterface
public interface StatementAnalyzer {

    /**
     * Extracts plan graph.
     * @param node the statement information
     * @return extracted plan graph
     * @throws PlanGraphException if error was occurred while extracting plan graph
     */
    PlanGraph analyze(@Nonnull ObjectNode node) throws PlanGraphException;
}
