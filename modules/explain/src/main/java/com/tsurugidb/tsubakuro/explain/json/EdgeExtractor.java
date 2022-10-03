package com.tsurugidb.tsubakuro.explain.json;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts connection to exchanges from relational operators.
 */
public interface EdgeExtractor {

    /**
     * Extracts reference of input exchanges.
     * @param object the JSON object
     * @return the input exchange references, or {@code empty} if this does not refer input exchanges
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default List<String> getInputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return List.of();
    }

    /**
     * Extracts reference of output exchanges.
     * @param object the JSON object
     * @return the output exchange references, or {@code empty} if this does not refer output exchanges
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default List<String> getOutputExchanges(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return List.of();
    }
}
