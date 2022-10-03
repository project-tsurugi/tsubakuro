package com.tsurugidb.tsubakuro.explain.json;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts edges from {@code offer}.
 */
public class OfferEdgeExtractor implements EdgeExtractor {

    @Override
    public List<String> getOutputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "offer");
        var bindingRef = PropertyExtractorUtil.getDestinationExchangeRef(object);
        return List.of(bindingRef);
    }
}
