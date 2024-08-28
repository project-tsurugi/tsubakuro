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
