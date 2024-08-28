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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts execution plan node properties.
 */
public interface PropertyExtractor {

    /**
     * Extracts node title from JSON object.
     * @param object the JSON object
     * @return the node title, or empty if title is not defined
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default Optional<String> getTitle(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return Optional.empty();
    }

    /**
     * Extracts attributes from JSON object.
     * @param object the JSON object
     * @return the attributes
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return Map.of();
    }
}
