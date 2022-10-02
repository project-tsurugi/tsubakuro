package com.tsurugidb.tsubakuro.explain.json;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts edges from {@code take_flat} and {@code take_group}.
 */
public class TakeExchangeEdgeExtractor implements EdgeExtractor {

    @Override
    public List<String> getInputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, Set.of("take_flat", "take_group"));
        var bindingRef = PropertyExtractorUtil.getSourceExchangeRef(object);
        return List.of(bindingRef);
    }
}
