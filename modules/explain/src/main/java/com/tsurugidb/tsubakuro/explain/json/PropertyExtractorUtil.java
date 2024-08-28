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

import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

final class PropertyExtractorUtil {

    static Map<String, String> attributes() {
        return Map.of();
    }

    static Map<String, String> attributes(@Nonnull String key, @Nonnull String value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        return Map.of(key, value);
    }

    static Map<String, String> attributes(@Nonnull String... keyOrValues) {
        Objects.requireNonNull(keyOrValues);
        if (keyOrValues.length % 2 != 0) {
            throw new IllegalArgumentException("invalid key value pairs");
        }
        // preserves entry order
        var results = new LinkedHashMap<String, String>();
        for (int i = 0, n = keyOrValues.length; i < n; i += 2) {
            var key = Objects.requireNonNull(keyOrValues[i]);
            var value = Objects.requireNonNull(keyOrValues[i + 1]);
            results.put(key, value);
        }
        return Collections.unmodifiableMap(results);
    }

    static Map<String, String> attributes(@Nonnull Map<String, String> map) {
        Objects.requireNonNull(map);
        if (map.size() <= 1) {
            return Map.copyOf(map);
        }
        return Collections.unmodifiableMap(map);
    }

    static String getSourceExchangeRef(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        var source = JsonUtil.getObject(node, "source");
        var bindingRef = extractExchangeRef(source);
        return bindingRef;
    }

    static String getDestinationExchangeRef(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        var destination = JsonUtil.getObject(node, "destination");
        var bindingRef = extractExchangeRef(destination);
        return bindingRef;
    }

    private static String extractExchangeRef(ObjectNode node) throws PlanGraphException {
        var binding = JsonUtil.getObject(node, "binding");
        JsonUtil.checkKind(binding, "exchange");
        var bindingRef = JsonUtil.getThis(binding);
        return bindingRef;
    }

    static Optional<String> findSourceExchangeRef(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        var source = JsonUtil.getObject(node, "source");
        var binding = JsonUtil.getObject(source, "binding");
        if (Objects.equals(JsonUtil.getKind(binding), "exchange")) {
            var bindingRef = JsonUtil.getThis(binding);
            return Optional.of(bindingRef);
        }
        return Optional.empty();
    }

    static class IndexInfo {

        final String table;

        final @Nullable String index;

        final Set<String> directColumns;

        final Set<String> features;

        IndexInfo(
                @Nonnull String table,
                @Nullable String index,
                @Nonnull Set<String> directColumns,
                @Nonnull Set<String> features) {
            Objects.requireNonNull(table);
            Objects.requireNonNull(features);
            this.table = table;
            this.index = index;
            this.directColumns = Set.copyOf(directColumns);
            this.features = Set.copyOf(features);
        }

        Map<String, String> toAttributes() {
            if (index == null) {
                return attributes(
                        "source", "table",
                        "table", table);
            }
            return attributes(
                    "source", "index",
                    "table", table,
                    "index", index);

        }
    }

    static IndexInfo getSourceIndex(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        var info = findSourceIndex(node);
        if (info.isPresent()) {
            return info.get();
        }
        var kind = JsonUtil.getKind(node);
        var self = JsonUtil.getThis(node);
        throw new PlanGraphException(MessageFormat.format(
                "failed to retrieve source index information: kind={0}, reference={1}",
                kind,
                self));
    }

    static Optional<IndexInfo> findSourceIndex(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        var source = JsonUtil.getObject(node, "source");
        var binding = JsonUtil.getObject(source, "binding");
        if (Objects.equals(JsonUtil.getKind(binding), "index")) {
            var info = extractIndexInfo(binding);
            return Optional.of(info);
        }
        return Optional.empty();
    }

    static IndexInfo getDestinationIndex(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        var source = JsonUtil.getObject(node, "destination");
        var destination = JsonUtil.getObject(source, "binding");
        JsonUtil.checkKind(destination, "index");
        var info = extractIndexInfo(destination);
        return info;
    }

    private static IndexInfo extractIndexInfo(@Nonnull JsonNode binding) throws PlanGraphException {
        assert JsonUtil.getKind(binding).equals("index");
        var table = JsonUtil.getString(binding, "table");
        var features = new LinkedHashSet<>(JsonUtil.getStringArray(binding, "features"));
        String index = null;
        Set<String> columns = new LinkedHashSet<>();
        if (!features.contains("primary")) {
            index = JsonUtil.getString(binding, "simple_name");
            for (var key : JsonUtil.getArray(binding, "keys")) {
                var column = JsonUtil.getString(key, "column");
                columns.add(column);
            }
            columns.addAll(JsonUtil.getStringArray(binding, "values"));
        }
        return new IndexInfo(table, index, columns, features);
    }

    static boolean hasScanRange(@Nonnull JsonNode node) throws PlanGraphException {
        return hasScanBound(node, "lower") || hasScanBound(node, "upper");
    }

    private static boolean hasScanBound(@Nonnull JsonNode node, @Nonnull String name) throws PlanGraphException {
        var endpoint = JsonUtil.get(node, name);
        return !Objects.equals(JsonUtil.getKind(endpoint), "unbound");
    }

    private PropertyExtractorUtil() {
        throw new AssertionError();
    }
}
