package com.tsurugidb.tsubakuro.sql;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlCommon;

/**
 * Represents metadata of executable statements.
 */
public interface StatementMetadata extends RelationMetadata {

    /**
     * Returns the result set columns information of the statement.
     * @return the result set columns information, or empty if it does not provided
     *      (the statement may not return result sets, or the feature is disabled)
     */
    @Override
    List<? extends SqlCommon.Column> getColumns();

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
