package com.tsurugidb.tsubakuro.explain.json;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code find}.
 */
public class FindPropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("scan");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "find");
        var info = PropertyExtractorUtil.getSourceIndex(object);
        var results = new LinkedHashMap<String, String>();
        results.putAll(info.toAttributes());
        results.put("access", "point");
        return results;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
