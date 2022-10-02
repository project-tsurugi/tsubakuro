package com.tsurugidb.tsubakuro.explain.json;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An implementation of {@link PropertyExtractor} which provides constant title and attributes.
 */
public class ConstantPropertyExtractor implements PropertyExtractor {

    private final String title;

    private final Map<String, String> attributes;

    /**
     * Creates a new instance.
     * @param title the node title, or {@code null} to use node kind name as the title
     * @param attributes the node attributes
     */
    public ConstantPropertyExtractor(@Nullable String title, @Nonnull Map<String, String> attributes) {
        Objects.requireNonNull(attributes);
        this.title = title;
        this.attributes = Map.copyOf(attributes);
    }

    @Override
    public Optional<String> getTitle(@Nonnull ObjectNode object) {
        Objects.requireNonNull(object);
        return Optional.of(title);
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) {
        Objects.requireNonNull(object);
        return attributes;
    }

    @Override
    public String toString() {
        return String.format("ConstantPropertyExtractor [title=%s, attributes=%s]", title, attributes); //$NON-NLS-1$
    }
}
