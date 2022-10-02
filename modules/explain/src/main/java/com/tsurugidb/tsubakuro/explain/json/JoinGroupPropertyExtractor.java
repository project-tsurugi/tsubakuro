package com.tsurugidb.tsubakuro.explain.json;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code join_group}.
 */
public class JoinGroupPropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("join");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "join_group");
        var kind = JsonUtil.getString(object, "operator_kind");
        return Map.of(
                "join-type", kind,
                "source", "flow",
                "access", "merge");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
