/*
 * Copyright 2023-2026 Project Tsurugi.
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code group}.
 */
public class GroupExchangePropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("group");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "group");
        var grouped = !JsonUtil.getArray(object, "group_keys").isEmpty();
        var sorted = !JsonUtil.getArray(object, "sort_keys").isEmpty();
        var rowSlice = PropertyExtractorUtil.findRowSlice(object);
        var results = new LinkedHashMap<String, String>();
        results.put("whole", String.valueOf(!grouped));
        results.put("sorted", String.valueOf(sorted));
        rowSlice.ifPresent(it -> results.putAll(it.toAttributes()));
        return results;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
