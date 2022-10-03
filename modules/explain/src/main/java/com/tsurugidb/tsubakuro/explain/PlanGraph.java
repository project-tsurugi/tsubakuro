package com.tsurugidb.tsubakuro.explain;

import java.util.Set;

/**
 * Represents an execution plan.
 */
public interface PlanGraph {

    /**
     * Returns the all nodes in this graph.
     * @return the all nodes
     */
    Set<? extends PlanNode> getNodes();

    /**
     * Returns the all source nodes in this graph.
     * @return the all source nodes
     */
    Set<? extends PlanNode> getSources();

    /**
     * Returns the all destination nodes in this graph.
     * @return the all destination nodes
     */
    Set<? extends PlanNode> getDestinations();
}
