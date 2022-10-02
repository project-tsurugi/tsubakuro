package com.tsurugidb.tsubakuro.explain.json;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts edges from {@code join_find} or {@code join_scan}.
 */
public class JoinExchangeEdgeExtractor implements EdgeExtractor {

    @Override
    public List<String> getInputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, Set.of("join_find", "join_scan"));
        return PropertyExtractorUtil.findSourceExchangeRef(object)
                .map(List::of)
                .orElse(List.of());
    }
}
