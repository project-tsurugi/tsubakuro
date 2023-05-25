package com.tsurugidb.tsubakuro.kvs;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.tsurugidb.kvs.proto.KvsData;

/**
 * Utilities for values.
 *
 * <h3> Value type mapping </h3>
 *
 * <table>
 *   <caption> value type mapping </caption>
 *   <thead>
 *     <tr>
 *       <th> column type </th>
 *       <th> runtime type </th>
 *       <th> transport field </th>
 *       <th> conversion </th>
 *     </tr>
 *   </thead>
 *   <tbody>
 *     <tr>
 *       <td> {@code BOOLEAN} </td>
 *       <td> {@code boolean} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getBooleanValue() value.getBooleanValue()} </td>
 *       <td> {@link #of(boolean)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code INT} </td>
 *       <td> {@code int} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getInt4Value() value.getInt4Value()} </td>
 *       <td> {@link #of(int)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code BIGINT} </td>
 *       <td> {@code long} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getInt8Value() value.getInt8Value()} </td>
 *       <td> {@link #of(long)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code FLOAT} </td>
 *       <td> {@code float} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getFloat4Value() value.getFloat4Value()} </td>
 *       <td> {@link #of(float)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code DOUBLE PRECISION} </td>
 *       <td> {@code double} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getFloat8Value() value.getFloat8Value()} </td>
 *       <td> {@link #of(double)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code DECIMAL} </td>
 *       <td> {@link BigDecimal} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getDecimalValue() value.getDecimalValue()} </td>
 *       <td> {@link #of(BigDecimal)} </td>
 *     </tr>
 *
 *     <tr>
 *       <td> {@code CHAR}, {@code VARCHAR} </td>
 *       <td> {@link String} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getCharacterValue() value.getCharacterValue()} </td>
 *       <td> {@link #of(String)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code BINARY}, {@code VARBINARY} </td>
 *       <td> {@code byte[]} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getOctetValue() value.getOctetValue()} </td>
 *       <td> {@link #of(byte[])} </td>
 *     </tr>

 *     <tr>
 *       <td> {@code DATE} </td>
 *       <td> {@link LocalDate} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getDateValue() value.getDateValue()} </td>
 *       <td> {@link #of(LocalDate)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code TIME} </td>
 *       <td> {@link LocalTime} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getTimeOfDayValue() value.getTimeOfDayValue()} </td>
 *       <td> {@link #of(LocalTime)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code TIMESTAMP} </td>
 *       <td> {@link LocalDateTime} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getTimePointValue() value.getTimePointValue()} </td>
 *       <td> {@link #of(LocalDateTime)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code TIME WITH TIMEZONE} </td>
 *       <td> {@link OffsetTime} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getTimeOfDayWithTimeZoneValue() value.getTimeOfDayWithTimeZoneValue()} </td>
 *       <td> {@link #of(OffsetTime)} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code TIMESTAMP WITH TIMEZONE} </td>
 *       <td> {@link OffsetDateTime} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getTimePointWithTimeZoneValue() value.getTimePointWithTimeZoneValue()} </td>
 *       <td> {@link #of(OffsetDateTime)} </td>
 *     </tr>
 *
 *     <tr>
 *       <td> N/A </td>
 *       <td> {@code null} </td>
 *       <td> {@link com.tsurugidb.kvs.proto.KvsData.Value#getValueCase() value.getValueCase() == ValueCase.VALUE_NOT_SET} </td>
 *       <td> {@link #ofNull()} </td>
 *     </tr>
 *   </tbody>
 * </table>
 */
public final class Values {

    /**
     * Converts a transport value to the runtime style.
     *
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static @Nullable Object toObject(@Nonnull KvsData.Value value) {
        Objects.requireNonNull(value);
        switch (value.getValueCase()) {
        case VALUE_NOT_SET:
            return null;
        case BOOLEAN_VALUE:
            return value.getBooleanValue();
        case INT4_VALUE:
            return value.getInt4Value();
        case INT8_VALUE:
            return value.getInt8Value();
        case FLOAT4_VALUE:
            return value.getFloat4Value();
        case FLOAT8_VALUE:
            return value.getFloat8Value();
        case DECIMAL_VALUE:
            return toObject(value.getDecimalValue());
        case CHARACTER_VALUE:
            return value.getCharacterValue();
        case OCTET_VALUE:
            return toObject(value.getOctetValue());
        case DATE_VALUE:
            return LocalDate.ofEpochDay(value.getDateValue());
        case TIME_OF_DAY_VALUE:
            return LocalTime.ofNanoOfDay(value.getTimeOfDayValue());
        case TIME_POINT_VALUE:
            return toObject(value.getTimePointValue());
        case TIME_OF_DAY_WITH_TIME_ZONE_VALUE:
            return toObject(value.getTimeOfDayWithTimeZoneValue());
        case TIME_POINT_WITH_TIME_ZONE_VALUE:
            return toObject(value.getTimePointWithTimeZoneValue());
        case DATETIME_INTERVAL_VALUE:
            // FIXME: impl
            throw new UnsupportedOperationException(value.getValueCase().toString());
        case LIST_VALUE:
            return toObject(value.getListValue());
        case RECORD_VALUE:
            return toObject(value.getRecordValue());
        }
        throw new IllegalArgumentException(value.getValueCase().toString());
    }

    /**
     * Converts a transport decimal value to the runtime style.
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static BigDecimal toObject(@Nonnull KvsData.Decimal value) {
        Objects.requireNonNull(value);
        return new BigDecimal(
                new BigInteger(value.getUnscaledValue().toByteArray()),
                -value.getExponent());
    }

    /**
     * Converts a transport octet value to the runtime style.
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static byte[] toObject(@Nonnull ByteString value) {
        Objects.requireNonNull(value);
        return value.toByteArray();
    }

    /**
     * Converts a transport time point value to the runtime style.
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static LocalDateTime toObject(@Nonnull KvsData.TimePoint value) {
        Objects.requireNonNull(value);
        return LocalDateTime.ofEpochSecond(value.getOffsetSeconds(), value.getNanoAdjustment(), ZoneOffset.UTC);
    }

    /**
     * Converts a transport time of day with timezone value to the runtime style.
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static OffsetTime toObject(@Nonnull KvsData.TimeOfDayWithTimeZone value) {
        Objects.requireNonNull(value);
        return OffsetTime.of(
                LocalTime.ofNanoOfDay(value.getOffsetNanoseconds()),
                ZoneOffset.ofTotalSeconds(60 * value.getTimeZoneOffset()));
    }

    /**
     * Converts a transport time point with timezone value to the runtime style.
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static OffsetDateTime toObject(@Nonnull KvsData.TimePointWithTimeZone value) {
        Objects.requireNonNull(value);
        var zone = ZoneOffset.ofTotalSeconds(60 * value.getTimeZoneOffset());
        return OffsetDateTime.of(
                LocalDateTime.ofEpochSecond(
                        value.getOffsetSeconds(),
                        value.getNanoAdjustment(),
                        zone),
                zone);
    }

    /**
     * Converts a transport list value to the runtime style.
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static List<?> toObject(@Nonnull KvsData.List value) {
        Objects.requireNonNull(value);
        if (value.getValuesCount() == 0) {
            return List.of();
        }
        if (value.getValuesCount() == 1) {
            return List.of(toObject(value.getValues(0)));
        }
        var results = new ArrayList<>(value.getValuesCount());
        for (var entry : value.getValuesList()) {
            results.add(toObject(entry));
        }
        return Collections.unmodifiableList(results);
    }

    /**
     * Converts a transport record value to the runtime style.
     * @param value the source value
     * @return the corresponding runtime value.
     */
    public static Record toObject(@Nonnull KvsData.Record value) {
        Objects.requireNonNull(value);
        return new Record(value);
    }

    /**
     * Converts a runtime value to transport style.
     * <p>
     * This only accepts the valid runtime type value or its wrapper type values.
     * </p>
     * @param object the source value
     * @return the corresponding transport value
     * @throws IllegalArgumentException if the input value is not supported
     */
    public static KvsData.Value toValue(@Nullable Object object) {
        if (object == null) {
            return ofNull();
        }

        if (object instanceof Boolean) {
            return of((Boolean) object);
        }
        if (object instanceof Integer) {
            return of((Integer) object);
        }
        if (object instanceof Long) {
            return of((Long) object);
        }
        if (object instanceof Float) {
            return of((Float) object);
        }
        if (object instanceof Double) {
            return of((Double) object);
        }
        if (object instanceof BigDecimal) {
            return of((BigDecimal) object);
        }

        if (object instanceof String) {
            return of((String) object);
        }
        if (object instanceof byte[]) {
            return of((byte[]) object);
        }

        if (object instanceof LocalDate) {
            return of((LocalDate) object);
        }
        if (object instanceof LocalTime) {
            return of((LocalTime) object);
        }
        if (object instanceof LocalDateTime) {
            return of((LocalDateTime) object);
        }

        if (object instanceof OffsetTime) {
            return of((OffsetTime) object);
        }
        if (object instanceof OffsetDateTime) {
            return of((OffsetDateTime) object);
        }

        if (object instanceof List<?>) {
            return of((List<?>) object);
        }
        if (object instanceof Record) {
            return of((Record) object);
        }

        throw new IllegalArgumentException(MessageFormat.format(
                "unsupported value: {0}",
                object));
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code NULL}.
     * @return the corresponding value
     */
    public static KvsData.Value ofNull() {
        return KvsData.Value.getDefaultInstance();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code BOOL}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(boolean value) {
        return KvsData.Value.newBuilder()
                .setBooleanValue(value)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code INT}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(int value) {
        return KvsData.Value.newBuilder()
                .setInt4Value(value)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code BIGINT}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(long value) {
        return KvsData.Value.newBuilder()
                .setInt8Value(value)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code FLOAT}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(float value) {
        return KvsData.Value.newBuilder()
                .setFloat4Value(value)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code DOUBLE PRECISION}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(double value) {
        return KvsData.Value.newBuilder()
                .setFloat8Value(value)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code DECIMAL}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull BigDecimal value) {
        return KvsData.Value.newBuilder()
                .setDecimalValue(KvsData.Decimal.newBuilder()
                        .setUnscaledValue(ByteString.copyFrom(value.unscaledValue().toByteArray()))
                        .setExponent(-value.scale()))
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code CHAR, VARCHAR}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull String value) {
        return KvsData.Value.newBuilder()
                .setCharacterValue(value)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code BINARY, VARBINARY}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull byte[] value) {
        Objects.requireNonNull(value);
        return ofBinary(ByteString.copyFrom(value));
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code BINARY, VARBINARY}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value ofBinary(@Nonnull ByteString value) {
        Objects.requireNonNull(value);
        return KvsData.Value.newBuilder()
                .setOctetValue(value)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code DATE}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull LocalDate value) {
        Objects.requireNonNull(value);
        return KvsData.Value.newBuilder()
                .setDateValue(value.toEpochDay())
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code TIME}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull LocalTime value) {
        Objects.requireNonNull(value);
        return KvsData.Value.newBuilder()
                .setTimeOfDayValue(value.toNanoOfDay())
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code TIMESTAMP}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull LocalDateTime value) {
        Objects.requireNonNull(value);
        return KvsData.Value.newBuilder()
                .setTimePointValue(KvsData.TimePoint.newBuilder()
                        .setOffsetSeconds(value.toEpochSecond(ZoneOffset.UTC))
                        .setNanoAdjustment(value.getNano()))
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code TIME WITH TIMEZONE}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull OffsetTime value) {
        Objects.requireNonNull(value);
        return KvsData.Value.newBuilder()
                .setTimeOfDayWithTimeZoneValue(KvsData.TimeOfDayWithTimeZone.newBuilder()
                        .setOffsetNanoseconds(value.toLocalTime().toNanoOfDay())
                        .setTimeZoneOffset(value.getOffset().getTotalSeconds() / 60))
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} which represents {@code TIMESTAMP WITH TIMEZONE}.
     * @param value the value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull OffsetDateTime value) {
        Objects.requireNonNull(value);
        return KvsData.Value.newBuilder()
                .setTimePointWithTimeZoneValue(KvsData.TimePointWithTimeZone.newBuilder()
                        .setOffsetSeconds(value.toEpochSecond())
                        .setNanoAdjustment(value.getNano())
                        .setTimeZoneOffset(value.getOffset().getTotalSeconds() / 60))
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} from list of runtime values.
     * @param objects the runtime values
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull List<?> objects) {
        Objects.requireNonNull(objects);
        var list = KvsData.List.newBuilder();
        for (var object : objects) {
            var value = toValue(object);
            list.addValues(value);
        }
        return KvsData.Value.newBuilder()
                .setListValue(list)
                .build();
    }

    /**
     * Returns a {@link com.tsurugidb.kvs.proto.KvsData.Value KvsData.Value} from a record.
     * @param record the runtime value
     * @return the corresponding value
     */
    public static KvsData.Value of(@Nonnull Record record) {
        Objects.requireNonNull(record);
        return KvsData.Value.newBuilder()
                .setRecordValue(record.getEntity())
                .build();
    }

    private Values() {
        throw new AssertionError();
    }
}
