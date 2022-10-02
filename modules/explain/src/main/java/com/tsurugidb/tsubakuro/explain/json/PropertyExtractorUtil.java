package com.tsurugidb.tsubakuro.explain.json;

import java.text.MessageFormat;
import java.util.LinkedHashSet;
import java.util.List;
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

        List<Map.Entry<String, String>> toAttributes() {
            if (index == null) {
                return List.of(
                        Map.entry("source", "table"),
                        Map.entry("table", table));
            }
            return List.of(
                    Map.entry("source", "index"),
                    Map.entry("table", table),
                    Map.entry("index", index));

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
