package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Represents metadata of executable statements.
 */
public interface StatementMetadata extends RelationMetadata {

    /**
     * Returns execution plan information as character string.
     * @return the execution plan text
     */
    String getPlanText();

    /**
     * Returns execution plan information of the statement as the given type.
     * <p>
     * {@link #getPlan(Class) getPlan(java.lang.String.class)} is always allowed, it will return
     * {@link #getPlanText()}.
     * </p>
     * @param <T> the execution plan structure type
     * @param type the execution plan structure type
     * @return execution plan information, or {@code empty} if the given type is not supported
     */
    default <T> Optional<T> getPlan(@Nonnull Class<T> type) {
        Objects.requireNonNull(type);
        if (type == String.class) {
            return Optional.of(type.cast(getPlanText()));
        }
        return Optional.empty();
    }
}
