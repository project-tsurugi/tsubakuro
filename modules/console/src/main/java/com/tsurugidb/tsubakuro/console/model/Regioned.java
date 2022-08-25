package com.tsurugidb.tsubakuro.console.model;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A value with region.
 * @param <V> the value type
 */
public final class Regioned<V> {

    private final V value;

    private final Region region;

    /**
     * Creates a new instance.
     * @param value the actual value
     * @param region the region of the value
     */
    public Regioned(@Nullable V value, @Nonnull Region region) {
        Objects.requireNonNull(region);
        this.value = value;
        this.region = region;
    }

    /**
     * Returns the value.
     * @return the value
     */
    public @Nullable V getValue() {
        return value;
    }

    /**
     * Returns the region.
     * @return the region
     */
    public Region getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
