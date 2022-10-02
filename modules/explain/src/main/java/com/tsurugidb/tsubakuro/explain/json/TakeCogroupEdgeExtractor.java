package com.tsurugidb.tsubakuro.explain.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts edges from {@code take_cogroup}.
 */
public class TakeCogroupEdgeExtractor implements EdgeExtractor {

    @Override
    public List<String> getInputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "take_cogroup");
        var results = new ArrayList<String>();
        var groups = JsonUtil.getArray(object, "groups");
        for (var group : groups) {
            var bindingRef = PropertyExtractorUtil.getSourceExchangeRef(group);
            results.add(bindingRef);
        }
        return results;
    }
}
