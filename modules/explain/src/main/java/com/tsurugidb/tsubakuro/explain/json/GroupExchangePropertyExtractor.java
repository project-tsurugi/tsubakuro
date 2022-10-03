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
        var limit = JsonUtil.findInteger(object, "limit");
        var results = new LinkedHashMap<String, String>();
        results.put("whole", String.valueOf(!grouped));
        results.put("sorted", String.valueOf(sorted));
        limit.ifPresent(it -> results.put("limit", String.valueOf(it)));
        return results;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
