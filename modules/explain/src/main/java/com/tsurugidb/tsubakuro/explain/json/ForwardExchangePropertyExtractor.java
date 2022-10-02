package com.tsurugidb.tsubakuro.explain.json;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code forward}.
 */
public class ForwardExchangePropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("forward");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "forward");
        var limit = JsonUtil.findInteger(object, "limit");
        if (limit.isEmpty()) {
            return Map.of();
        }
        return Map.of("limit", String.valueOf(limit.getAsLong()));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
