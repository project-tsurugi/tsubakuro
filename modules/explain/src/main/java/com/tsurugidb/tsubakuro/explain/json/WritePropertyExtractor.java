package com.tsurugidb.tsubakuro.explain.json;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code write}.
 */
public class WritePropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("write");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "write");
        var info = PropertyExtractorUtil.getDestinationIndex(object);
        var kind = JsonUtil.getString(object, "operator_kind");
        return PropertyExtractorUtil.attributes(
                "write-kind", kind,
                "table", info.table);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
