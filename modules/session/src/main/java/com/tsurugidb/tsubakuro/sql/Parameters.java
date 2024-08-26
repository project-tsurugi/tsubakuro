package com.tsurugidb.tsubakuro.sql;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;

/**
 * Utilities of {@link com.tsurugidb.sql.proto.SqlRequest.Parameter Parameter}.
 */
public final class Parameters {

    /**
     * Returns a new {@code NULL} parameter.
     * @param name the place-holder name
     * @return the created place-holder
     */
    public static SqlRequest.Parameter ofNull(@Nonnull String name) {
        Objects.requireNonNull(name);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, boolean value) {
        Objects.requireNonNull(name);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, long value) {
        Objects.requireNonNull(name);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, int value) {
        Objects.requireNonNull(name);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, float value) {
        Objects.requireNonNull(name);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, double value) {
        Objects.requireNonNull(name);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull BigDecimal value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setDecimalValue(SqlCommon.Decimal.newBuilder()
                        .setUnscaledValue(ByteString.copyFrom(value.unscaledValue().toByteArray()))
                        .setExponent(-value.scale()))
                .build();
    }

    /**
     * Returns a new {@code CHARACTER} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull String value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull byte[] value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull ByteString value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setOctetValue(value)
                .build();
    }

    /**
     * <em>This method is not yet implemented:</em>
     * Returns a new {@code BIT} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull boolean[] value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);

        var bytes = new byte[(value.length + 8 - 1) / 8];
        for (int i = 0; i < value.length; i++) {
            int byteOffset = i / 8;
            int bitOffset = i % 8;
            bytes[byteOffset] |= value[i] ? (1 << bitOffset) : 0;
        }

        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setBitValue(SqlCommon.Bit.newBuilder()
                        .setPacked(ByteString.copyFrom(bytes))
                        .setSize(value.length))
                .build();
    }

    /**
     * <em>This method is not yet implemented:</em>
     * Returns a new {@code BIT} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull SqlCommon.Bit value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull LocalDate value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull LocalTime value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull LocalDateTime value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setTimePointValue(SqlCommon.TimePoint.newBuilder()
                        .setOffsetSeconds(value.toEpochSecond(ZoneOffset.UTC))
                        .setNanoAdjustment(value.getNano()))
                .build();
    }

    /**
     * Returns a new {@code TIME_POINT} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull SqlCommon.TimePoint value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setTimePointValue(value)
                .build();
    }

    /**
     * Returns a new {@code TIME_OF_DAY_WITH_TIME_ZONE} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull OffsetTime value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setTimeOfDayWithTimeZoneValue(SqlCommon.TimeOfDayWithTimeZone.newBuilder()
                        .setOffsetNanoseconds(value.toLocalTime().toNanoOfDay())
                        .setTimeZoneOffset(value.getOffset().getTotalSeconds() / 60))
                .build();
    }

    /**
     * Returns a new {@code TIME_OF_DAY_WITH_TIME_ZONE} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull SqlCommon.TimeOfDayWithTimeZone value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setTimeOfDayWithTimeZoneValue(value)
                .build();
    }

    /**
     * Returns a new {@code TIME_POINT_WITH_TIME_ZONE} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull OffsetDateTime value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setTimePointWithTimeZoneValue(SqlCommon.TimePointWithTimeZone.newBuilder()
                        .setOffsetSeconds(value.toLocalDateTime().toEpochSecond(ZoneOffset.UTC))
                        .setNanoAdjustment(value.toLocalDateTime().getNano())
                        .setTimeZoneOffset(value.getOffset().getTotalSeconds() / 60))
                .build();
    }

    /**
     * Returns a new {@code TIME_POINT_WITH_TIME_ZONE} parameter.
     * @param name the place-holder name
     * @param value the value
     * @return the created place-holder
     */
    public static SqlRequest.Parameter of(@Nonnull String name, @Nonnull SqlCommon.TimePointWithTimeZone value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setTimePointWithTimeZoneValue(value)
                .build();
    }

    /**
     * Returns a new reference column parameter.
     * @param name the place-holder name
     * @param position the position of target column
     * @return the created place-holder
     */
    public static SqlRequest.Parameter referenceColumn(@Nonnull String name, int position) {
        Objects.requireNonNull(name);
        return SqlRequest.Parameter.newBuilder()
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
    public static SqlRequest.Parameter referenceColumn(@Nonnull String name, @Nonnull String columnName) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(columnName);
        return SqlRequest.Parameter.newBuilder()
                .setName(name)
                .setReferenceColumnName(columnName)
                .build();
    }

    private Parameters() {
        throw new AssertionError();
    }
}
