package com.tsurugidb.tsubakuro.explain;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * A basic implementation of {@link PlanGraph}.
 */
public class BasicPlanGraph implements PlanGraph {

    private final Set<PlanNode> nodes;

    /**
     * Creates a new instance.
     * @param nodes execution plan nodes
     */
    public BasicPlanGraph(@Nonnull Collection<? extends PlanNode> nodes) {
        Objects.requireNonNull(nodes);
        this.nodes = extract(nodes);
    }

    private static Set<PlanNode> extract(Collection<? extends PlanNode> nodes) {
        var saw = new HashSet<PlanNode>(nodes);
        var work = new ArrayDeque<PlanNode>(nodes);
        while (!work.isEmpty()) {
            var next = work.removeFirst();
            Stream.concat(next.getUpstreams().stream(), next.getDownstreams().stream())
                .filter(it -> !saw.contains(it))
                .forEach(it -> {
                    saw.add(it);
                    work.addFirst(it);
                });
        }
        return Set.copyOf(saw);
    }

    @Override
    public Set<? extends PlanNode> getNodes() {
        return nodes;
    }

    @Override
    public Set<? extends PlanNode> getSources() {
        return nodes.stream()
                .filter(it -> it.getUpstreams().isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<? extends PlanNode> getDestinations() {
        return nodes.stream()
                .filter(it -> it.getDownstreams().isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public String toString() {
        return String.format("PlanGraph [nodes=%s]", nodes);
    }
}
