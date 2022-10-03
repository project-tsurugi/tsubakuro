package com.tsurugidb.tsubakuro.explain;

import java.util.Map;
import java.util.Set;

/**
 * Represents a node of execution plan graph.
 */
public interface PlanNode {

    /**
     * Returns the kind of this element.
     * @return the element kind
     */
    String getKind();

    /**
     * Returns the title of this element.
     * @return the element title
     */
    String getTitle();

    /**
     * Returns the attributes of this element.
     * @return the attributes
     */
    Map<String, String> getAttributes();

    /**
     * Returns the upstream nodes.
     * @return the upstream nodes
     */
    Set<? extends PlanNode> getUpstreams();

    /**
     * Returns the downstream nodes.
     * @return the downstream nodes
     */
    Set<? extends PlanNode> getDownstreams();
}
