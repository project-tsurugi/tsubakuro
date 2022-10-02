package com.tsurugidb.tsubakuro.explain.json;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code aggregate}.
 */
public class AggregateExchangePropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return Optional.of("aggregate");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "aggregate");
        var grouped = !JsonUtil.getArray(object, "group_keys").isEmpty();
        return PropertyExtractorUtil.attributes(
                "whole", String.valueOf(!grouped),
                "incremental", "true");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
