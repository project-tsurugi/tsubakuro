package com.tsurugidb.tsubakuro.console.model;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a value.
 */
public final class Value {

    /**
     * Represents a kind of value.
     */
    public enum Kind {

        /**
         * is null.
         */
        NULL,

        /**
         * is a character string value.
         */
        CHARACTER,

        /**
         * is a numeric value.
         */
        NUMERIC,

        /**
         * is a boolean value.
         */
        BOOLEAN
    }

    private static final Value NULL_VALUE = new Value(null);

    private final Object entity;

    private Value(@Nullable Object entity) {
        this.entity = entity;
    }

    /**
     * Returns an object which represents {@code null}.
     * @return {@code null} value
     */
    public static Value of() {
        return NULL_VALUE;
    }

    /**
     * Wraps the value.
     * @param value the value
     * @return the wrapped value
     */
    public static Value of(@Nonnull String value) {
        Objects.requireNonNull(value);
        return new Value(value);
    }

    /**
     * Wraps the value.
     * @param value the value
     * @return the wrapped value
     */
    public static Value of(@Nonnull BigDecimal value) {
        Objects.requireNonNull(value);
        return new Value(value);
    }

    /**
     * Wraps the value.
     * @param value the value
     * @return the wrapped value
     */
    public static Value of(boolean value) {
        return new Value(value);
    }

    /**
     * Wraps the value.
     * @param value the value
     * @return the wrapped value
     */
    public static Value of(long value) {
        return of(BigDecimal.valueOf(value));
    }

    /**
     * Returns the value kind.
     * @return the value kind
     */
    public Kind getKind() {
        if (entity == null) {
            return Kind.NULL;
        }
        if (entity instanceof String) {
            return Kind.CHARACTER;
        }
        if (entity instanceof BigDecimal) {
            return Kind.NUMERIC;
        }
        if (entity instanceof Boolean) {
            return Kind.BOOLEAN;
        }
        throw new IllegalArgumentException(MessageFormat.format(
                "unsupported value; {0} ({1})",
                entity,
                entity.getClass().getName()));
    }

    /**
     * Returns character string value of this.
     * @return character string value, or {@code empty} if this does not represent it
     * @see #getKind()
     */
    public Optional<String> asCharacter() {
        return as(String.class);
    }

    /**
     * Returns numeric value of this.
     * @return numeric value, or {@code empty} if this does not represent it
     * @see #getKind()
     */
    public Optional<BigDecimal> asNumeric() {
        return as(BigDecimal.class);
    }

    /**
     * Returns boolean value of this.
     * @return boolean value, or {@code empty} if this does not represent it
     * @see #getKind()
     */
    public Optional<Boolean> asBoolean() {
        return as(Boolean.class);
    }

    private <V> Optional<V> as(Class<V> aClass) {
        return Optional.ofNullable(entity)
                .filter(aClass::isInstance)
                .map(aClass::cast);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entity);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Value other = (Value) obj;
        return Objects.equals(entity, other.entity);
    }

    @Override
    public String toString() {
        return String.valueOf(entity);
    }
}
