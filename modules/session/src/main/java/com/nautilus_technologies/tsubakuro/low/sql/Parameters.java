package com.nautilus_technologies.tsubakuro.low.sql;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.firebirdsql.decimal.Decimal128;

import com.google.protobuf.ByteString;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;

/**
 * Utilities of {@link com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet.Parameter Parameter}.
 */
public final class Parameters {

    /**
     * Returns a new {@code NULL} parameter.
     * @param name the place-holder name
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter ofNull(@Nonnull String name) {
        Objects.requireNonNull(name);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                // unset value
                // .clearValue()
                .build();
    }

    /**
     * Returns a new {@code BOOLEAN} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, boolean value) {
        Objects.requireNonNull(name);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setBooleanValue(value)
                .build();
    }

    /**
     * Returns a new {@code INT8} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, long value) {
        Objects.requireNonNull(name);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setInt8Value(value)
                .build();
    }

    /**
     * Returns a new {@code INT4} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, int value) {
        Objects.requireNonNull(name);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setInt4Value(value)
                .build();
    }

    /**
     * Returns a new {@code FLOAT4} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, float value) {
        Objects.requireNonNull(name);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setFloat4Value(value)
                .build();
    }

    /**
     * Returns a new {@code FLOAT8} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, double value) {
        Objects.requireNonNull(name);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setFloat8Value(value)
                .build();
    }

    /**
     * Returns a new {@code DECIMAL} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull BigDecimal value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        var bytes = ByteString.copyFrom(Decimal128.valueOf(value).toBytes());
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setDecimalValue(bytes)
                .build();
    }

    /**
     * Returns a new {@code CHARACTER} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull String value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setCharacterValue(value)
                .build();
    }

    /**
     * Returns a new {@code OCTET} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull byte[] value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setOctetValue(ByteString.copyFrom(value))
                .build();
    }

    /**
     * Returns a new {@code OCTET} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull ByteString value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setOctetValue(value)
                .build();
    }

    /**
     * Returns a new {@code BIT} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull boolean[] value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);

        var bytes = new byte[(value.length + 8 - 1) / 8];
        for (int i = 0; i < value.length; i++) {
            int byteOffset = i / 8;
            int bitOffset = i % 8;
            bytes[byteOffset] |= value[i] ? (1 << bitOffset) : 0;
        }

        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setBitValue(CommonProtos.Bit.newBuilder()
                        .setPacked(ByteString.copyFrom(bytes))
                        .setSize(value.length))
                .build();
    }

    /**
     * Returns a new {@code BIT} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull CommonProtos.Bit value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setBitValue(value)
                .build();
    }

    /**
     * Returns a new {@code DATE} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull LocalDate value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setDateValue(value.toEpochDay())
                .build();
    }

    /**
     * Returns a new {@code TIME_OF_DAY} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull LocalTime value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setTimeOfDayValue(value.toNanoOfDay())
                .build();
    }

    /**
     * Returns a new {@code TIME_POINT} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull Instant value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setTimePointValue(CommonProtos.TimePoint.newBuilder()
                        .setOffsetSeconds(value.getEpochSecond())
                        .setNanoAdjustment(value.getNano()))
                .build();
    }

    /**
     * Returns a new {@code TIME_POINT} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter of(@Nonnull String name, @Nonnull CommonProtos.TimePoint value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setTimePointValue(value)
                .build();
    }

    /**
     * Returns a new reference column parameter.
     * @param name the place-holder name
     * @param position the position of target column
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter referenceColumn(@Nonnull String name, int position) {
        Objects.requireNonNull(name);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setReferenceColumnPosition(position)
                .build();
    }

    /**
     * Returns a new reference column parameter.
     * @param name the place-holder name
     * @param columnName the name of target column
     * @return the created place-holder
     */
    public static RequestProtos.ParameterSet.Parameter referenceColumn(@Nonnull String name, @Nonnull String columnName) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(columnName);
        return RequestProtos.ParameterSet.Parameter.newBuilder()
                .setName(name)
                .setReferenceColumnName(columnName)
                .build();
    }

    private Parameters() {
        throw new AssertionError();
    }
}
