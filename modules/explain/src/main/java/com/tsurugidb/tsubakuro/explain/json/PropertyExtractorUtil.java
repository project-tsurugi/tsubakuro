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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

final class PropertyExtractorUtil {

    static final Logger LOG = LoggerFactory.getLogger(PropertyExtractorUtil.class);

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

    static Optional<String> findScanDirection(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        if (isAbsent(node.get("direction"))) {
            return Optional.empty();
        }
        var direction = JsonUtil.getString(node, "direction");
        if (Objects.equals(direction, "forward") || Objects.equals(direction, "backward")) {
            return Optional.of(direction);
        }
        LOG.warn(MessageFormat.format(
                "unrecognized scan direction: {0}",
                direction));
        return Optional.empty();
    }

    static Set<String> findForwardOrdering(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        if (isAbsent(node.get("ordering"))) {
            return Set.of();
        }
        var results = new LinkedHashSet<String>();
        for (var value : JsonUtil.getStringArray(node, "ordering")) {
            if (Objects.equals(value, "respect_input") || Objects.equals(value, "preserve_output")) {
                results.add(value);
            } else {
                LOG.warn(MessageFormat.format(
                        "unrecognized forward ordering: {0}",
                        value));
            }
        }
        return results;
    }

    static class RowSliceInfo {

        final @Nullable String start;

        final @Nullable String count;

        RowSliceInfo(@Nullable String start, @Nullable String count) {
            this.start = start;
            this.count = count;
        }

        Map<String, String> toAttributes() {
            var results = new LinkedHashMap<String, String>();
            if (start != null) {
                results.put("offset", start);
            }
            if (count != null) {
                results.put("limit", count);
            }
            return attributes(results);
        }
    }

    /**
     * Returns the row slice of the operator.
     * <p>
     * This reads {@code "row_slice"} if it exists, or the legacy {@code "limit"} as its count otherwise.
     * Each value is an integer text for immediate values, or {@code "?"} for variables.
     * The immediate zero of {@code "start"} is the same as absent.
     * </p>
     * @param node the operator node
     * @return the row slice, or empty if neither start nor count is defined
     * @throws PlanGraphException if the row slice is malformed
     */
    static Optional<RowSliceInfo> findRowSlice(@Nonnull JsonNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        if (isAbsent(node.get("row_slice"))) {
            // for backward compatibility: older versions have "limit" instead of "row_slice"
            var limit = JsonUtil.findInteger(node, "limit");
            if (limit.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new RowSliceInfo(null, String.valueOf(limit.getAsLong())));
        }
        var rowSlice = JsonUtil.getObject(node, "row_slice");
        var start = findSliceValue(rowSlice, "start");
        var count = findSliceValue(rowSlice, "count");
        if (Objects.equals(start, "0")) {
            start = null;
        }
        if (start == null && count == null) {
            return Optional.empty();
        }
        return Optional.of(new RowSliceInfo(start, count));
    }

    private static @Nullable String findSliceValue(@Nonnull JsonNode rowSlice, @Nonnull String name)
            throws PlanGraphException {
        if (isAbsent(rowSlice.get(name))) {
            return null;
        }
        var value = JsonUtil.getObject(rowSlice, name);
        var kind = JsonUtil.getKind(value);
        if (Objects.equals(kind, "immediate")) {
            return String.valueOf(JsonUtil.getInteger(value, "element"));
        }
        if (Objects.equals(kind, "variable")) {
            // represents placeholders as "?"
            return "?";
        }
        LOG.warn(MessageFormat.format(
                "unrecognized value kind in row_slice.{0}: {1}",
                name,
                kind));
        return null;
    }

    private static boolean isAbsent(@Nullable JsonNode node) {
        return node == null || node.isMissingNode() || node.isNull();
    }

    private PropertyExtractorUtil() {
        throw new AssertionError();
    }
}
