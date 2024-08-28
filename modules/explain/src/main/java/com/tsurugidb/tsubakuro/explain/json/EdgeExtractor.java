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

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts connection to exchanges from relational operators.
 */
public interface EdgeExtractor {

    /**
     * Extracts reference of input exchanges.
     * @param object the JSON object
     * @return the input exchange references, or {@code empty} if this does not refer input exchanges
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default List<String> getInputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return List.of();
    }

    /**
     * Extracts reference of output exchanges.
     * @param object the JSON object
     * @return the output exchange references, or {@code empty} if this does not refer output exchanges
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default List<String> getOutputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return List.of();
    }
}
