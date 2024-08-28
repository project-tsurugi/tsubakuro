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
package com.tsurugidb.tsubakuro.explain;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
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
        var saw = new LinkedHashSet<PlanNode>(nodes);
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
        return Collections.unmodifiableSet(saw);
    }

    @Override
    public Set<? extends PlanNode> getNodes() {
        return nodes;
    }

    @Override
    public Set<? extends PlanNode> getSources() {
        return nodes.stream()
                .filter(it -> it.getUpstreams().isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public Set<? extends PlanNode> getDestinations() {
        return nodes.stream()
                .filter(it -> it.getDownstreams().isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public String toString() {
        return String.format("PlanGraph [nodes=%s]", nodes);
    }
}
