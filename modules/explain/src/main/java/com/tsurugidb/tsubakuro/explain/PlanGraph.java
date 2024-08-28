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
