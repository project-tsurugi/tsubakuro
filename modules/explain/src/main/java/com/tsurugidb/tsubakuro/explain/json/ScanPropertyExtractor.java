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
public class ScanPropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("scan");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "scan");
        var info = PropertyExtractorUtil.getSourceIndex(object);
        var access = PropertyExtractorUtil.hasScanRange(object) ? "range-scan" : "full-scan";
        var results = new LinkedHashMap<String, String>();
        results.put("access", access);
        info.toAttributes().forEach(it -> results.put(it.getKey(), it.getValue()));
        return results;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
