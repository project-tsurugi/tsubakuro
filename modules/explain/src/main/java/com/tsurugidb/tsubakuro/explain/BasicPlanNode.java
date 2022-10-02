package com.tsurugidb.tsubakuro.explain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

/**
 * A basic implementation of {@link PlanNode} which can configure up-/down-streams after creating object.
 */
public class BasicPlanNode implements PlanNode {

    private final String kind;

    private final String title;

    private final Map<String, String> attributes;

    private final Set<BasicPlanNode> upstreams = new LinkedHashSet<>();

    private final Set<BasicPlanNode> downstreams = new LinkedHashSet<>();

    /**
     * Creates a new instance.
     * @param kind the node kind
     * @param attributes the node attributes
     */
    public BasicPlanNode(@Nonnull String kind, @Nonnull Map<String, String> attributes) {
        this(kind, kind, attributes);
    }

    /**
     * Creates a new instance.
     * @param kind the node kind
     * @param title the node title
     * @param attributes the node attributes
     */
    public BasicPlanNode(@Nonnull String kind, @Nonnull String title, @Nonnull Map<String, String> attributes) {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(title);
        Objects.requireNonNull(attributes);
        this.kind = kind;
        this.title = title;
        this.attributes = copyMap(attributes);
    }

    private static Map<String, String> copyMap(Map<String, String> attributes) {
        if (attributes.size() <= 1) {
            return Map.copyOf(attributes);
        }
        // preserves entry order
        return Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    @Override
    public String getKind() {
        return kind;
    }

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public Set<? extends BasicPlanNode> getUpstreams() {
        return Collections.unmodifiableSet(upstreams);
    }

    @Override
    public Set<? extends BasicPlanNode> getDownstreams() {
        return Collections.unmodifiableSet(downstreams);
    }

    /**
     * Adds an upstream node.
     * @param other the upstream node to add
     */
    public void addUpstream(@Nonnull BasicPlanNode other) {
        Objects.requireNonNull(other);
        this.upstreams.add(other);
        other.downstreams.add(this);
    }

    /**
     * Adds an downstream node.
     * @param other the downstream node to add
     */
    public void addDownstream(@Nonnull BasicPlanNode other) {
        Objects.requireNonNull(other);
        this.downstreams.add(other);
        other.upstreams.add(this);
    }

    /**
     * Disconnects from all opposites and reconnect each other.
     */
    public void bypass() {
        var copyUpstreams = new ArrayList<>(upstreams);
        var copyDownstreams = new ArrayList<>(downstreams);
        upstreams.forEach(it -> it.downstreams.remove(this));
        downstreams.forEach(it -> it.upstreams.remove(this));
        upstreams.clear();
        downstreams.clear();
        for (var upstream : copyUpstreams) {
            for (var downstream : copyDownstreams) {
                upstream.addDownstream(downstream);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("{kind=%s, title=%s, attributes=%s}", //$NON-NLS-1$
                getKind(),
                getTitle(),
                getAttributes());
    }
}
