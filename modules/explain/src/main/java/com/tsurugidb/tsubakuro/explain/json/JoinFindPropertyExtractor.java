package com.tsurugidb.tsubakuro.explain.json;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code join_find}.
 */
public class JoinFindPropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("join");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "join_find");
        var info = PropertyExtractorUtil.findSourceIndex(object);
        var kind = JsonUtil.getString(object, "operator_kind");
        if (info.isEmpty()) {
            return Map.of(
                    "join-type", kind,
                    "source", "broadcast",
                    "access", "point");
        }
        var results = new LinkedHashMap<String, String>();
        results.put("join-type", kind);
        results.put("access", "point");
        info.get().toAttributes().forEach(it -> results.put(it.getKey(), it.getValue()));
        return results;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
