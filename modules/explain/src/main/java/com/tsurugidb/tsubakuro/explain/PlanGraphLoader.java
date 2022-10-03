package com.tsurugidb.tsubakuro.explain;

import javax.annotation.Nonnull;

/**
 * Analyzes execution plan.
 */
public interface PlanGraphLoader {

    /**
     * Analyzes the execution plan text and returns corresponding plan graph.
     * @param text the execution plan text
     * @return the analyzed plan graph
     * @throws PlanGraphException if the plan text is not valid to extract plan graph
     */
    PlanGraph load(@Nonnull String text) throws PlanGraphException;
}
