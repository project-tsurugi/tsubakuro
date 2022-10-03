package com.tsurugidb.tsubakuro.explain.json;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts execution plan node properties.
 */
public interface PropertyExtractor {

    /**
     * Extracts node title from JSON object.
     * @param object the JSON object
     * @return the node title, or empty if title is not defined
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default Optional<String> getTitle(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return Optional.empty();
    }

    /**
     * Extracts attributes from JSON object.
     * @param object the JSON object
     * @return the attributes
     * @throws PlanGraphException if error was occurred while extracting information from JSON object
     */
    default Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        return Map.of();
    }
}
