package com.tsurugidb.tsubakuro.sql;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlCommon.TypeInfo;

/**
 * Utilities to build {@link com.tsurugidb.sql.proto.SqlCommon.TypeInfo}.
 *
 * <p>
 * This helps to build a SQL type representation from the Java types.
 * </p>
 *
 * <h3>type mapping rule</h3>
 *
 * <table>
 *   <thead>
 *     <tr>
 *       <th> SQL type </th>
 *       <th> Java type </th>
 *     </tr>
 *   </thead>
 *   <tbody>
 *     <tr>
 *       <td> {@code BOOLEAN} </td>
 *       <td> {@code boolean}, {@link Boolean} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code INT4} </td>
 *       <td> {@code int}, {@link Integer} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code INT8} </td>
 *       <td> {@code long}, {@link Long} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code FLOAT4} </td>
 *       <td> {@code float}, {@link Float} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code FLOAT8} </td>
 *       <td> {@code double}, {@link Double} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code DECIMAL} </td>
 *       <td> {@link BigDecimal} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code CHARACTER} </td>
 *       <td> {@link String} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code OCTET} </td>
 *       <td> {@code byte[]}, {@link ByteString} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code BIT} </td>
 *       <td> {@code byte[]}, {@link com.tsurugidb.sql.proto.SqlCommon.Bit SqlCommon.Bit} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code DATE} </td>
 *       <td> {@link LocalDate}, {@link java.sql.Date} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code TIME_OF_DAY} </td>
 *       <td> {@link LocalTime}, {@link java.sql.Time} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code TIME_POINT} </td>
 *       <td> {@link Instant}, {@link java.sql.Timestamp} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code DATETIME_INTERVAL} </td>
 *       <td> {@link com.tsurugidb.sql.proto.SqlCommon.DateTimeInterval SqlCommon.DateTimeInterval} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code CLOB} </td>
 *       <td> {@link java.sql.Clob}, {@link com.tsurugidb.sql.proto.SqlCommon.Clob SqlCommon.Clob} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code BLOB} </td>
 *       <td> {@link java.sql.Clob}, {@link com.tsurugidb.sql.proto.SqlCommon.Blob SqlCommon.Blob} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code ARRAY} </td>
 *       <td> (the Java array type of corresponding types) </td>
 *     </tr>
 *     <tr>
 *       <td> {@code NULL} </td>
 *       <td> {@code void}, {@link Void} </td>
 *     </tr>
 *   </tbody>
 * </table>
 *
 * @see #typeOf(Class)
 */
public final class Types {

    private static final Map<Class<?>, SqlCommon.TypeInfo> TYPE_MAP = Map.ofEntries(
            // numeric
            Map.entry(boolean.class, wrap(SqlCommon.AtomType.BOOLEAN)),
            Map.entry(Boolean.class, wrap(SqlCommon.AtomType.BOOLEAN)),
            Map.entry(int.class, wrap(SqlCommon.AtomType.INT4)),
            Map.entry(Integer.class, wrap(SqlCommon.AtomType.INT4)),
            Map.entry(long.class, wrap(SqlCommon.AtomType.INT8)),
            Map.entry(Long.class, wrap(SqlCommon.AtomType.INT8)),
            Map.entry(float.class, wrap(SqlCommon.AtomType.FLOAT4)),
            Map.entry(Float.class, wrap(SqlCommon.AtomType.FLOAT4)),
            Map.entry(double.class, wrap(SqlCommon.AtomType.FLOAT8)),
            Map.entry(Double.class, wrap(SqlCommon.AtomType.FLOAT8)),
            Map.entry(BigDecimal.class, wrap(SqlCommon.AtomType.DECIMAL)),

            // sequence
            Map.entry(String.class, wrap(SqlCommon.AtomType.CHARACTER)),
            Map.entry(byte[].class, wrap(SqlCommon.AtomType.OCTET)),
            Map.entry(ByteString.class, wrap(SqlCommon.AtomType.OCTET)),
            Map.entry(boolean[].class, wrap(SqlCommon.AtomType.BIT)),
            Map.entry(SqlCommon.Bit.class, wrap(SqlCommon.AtomType.BIT)),

            // temporal
            Map.entry(LocalDate.class, wrap(SqlCommon.AtomType.DATE)),
            Map.entry(java.sql.Date.class, wrap(SqlCommon.AtomType.DATE)),
            Map.entry(LocalTime.class, wrap(SqlCommon.AtomType.TIME_OF_DAY)),
            Map.entry(java.sql.Time.class, wrap(SqlCommon.AtomType.TIME_OF_DAY)),
            Map.entry(LocalDateTime.class, wrap(SqlCommon.AtomType.TIME_POINT)),
            Map.entry(java.sql.Timestamp.class, wrap(SqlCommon.AtomType.TIME_POINT)),
            Map.entry(SqlCommon.TimePoint.class, wrap(SqlCommon.AtomType.TIME_POINT)),




            Map.entry(OffsetTime.class, wrap(SqlCommon.AtomType.TIME_OF_DAY_WITH_TIME_ZONE)),
            Map.entry(OffsetDateTime.class, wrap(SqlCommon.AtomType.TIME_POINT_WITH_TIME_ZONE)),



            Map.entry(DateTimeInterval.class, wrap(SqlCommon.AtomType.DATETIME_INTERVAL)),
            Map.entry(SqlCommon.DateTimeInterval.class, wrap(SqlCommon.AtomType.DATETIME_INTERVAL)),

            // LOB
            Map.entry(SqlCommon.Clob.class, wrap(SqlCommon.AtomType.CLOB)),
            Map.entry(java.sql.Clob.class, wrap(SqlCommon.AtomType.CLOB)),
            Map.entry(SqlCommon.Blob.class, wrap(SqlCommon.AtomType.BLOB)),
            Map.entry(java.sql.Blob.class, wrap(SqlCommon.AtomType.BLOB)),

            // special values
            Map.entry(void.class, wrap(SqlCommon.AtomType.UNKNOWN)),
            Map.entry(Void.class, wrap(SqlCommon.AtomType.UNKNOWN)));


    private Types() {
        throw new AssertionError();
    }

    /**
     * Returns corresponding type from the Java runtime type.
     * <p>
     * This is a synonym of {@link #typeOf(Class) typeOf(aClass)}.
     * </p>
     * @param aClass the Java runtime type
     * @return the corresponding type
     * @throws IllegalArgumentException if there is no corresponding type
     */
    public static SqlCommon.TypeInfo of(@Nonnull Class<?> aClass) {
        Objects.requireNonNull(aClass);
        return typeOf(aClass);
    }

    /**
     * Returns corresponding type from the Java runtime type.
     * @param aClass the Java runtime type
     * @return the corresponding type
     * @throws IllegalArgumentException if there is no corresponding type
     */
    public static SqlCommon.TypeInfo typeOf(@Nonnull Class<?> aClass) {
        Objects.requireNonNull(aClass);
        if (!TYPE_MAP.containsKey(aClass) && aClass.isArray()) {
            return array(of(aClass.getComponentType()));
        }
        TypeInfo type = TYPE_MAP.get(aClass);
        if (type == null) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "unrecognized type mapping: {0}",
                    aClass.getName()));
        }
        return type;
    }

    /**
     * Returns array type.
     * @param aClass the component type, resolved by {@link #typeOf(Class)}
     * @return the created type
     * @see #column(String, Class)
     */
    public static SqlCommon.TypeInfo array(@Nonnull Class<?> aClass) {
        Objects.requireNonNull(aClass);
        return array(typeOf(aClass));
    }

    /**
     * Returns array type.
     * @param componentType the component type
     * @return the created type
     */
    public static SqlCommon.TypeInfo array(@Nonnull SqlCommon.AtomType componentType) {
        Objects.requireNonNull(componentType);
        return array(componentType, 1);
    }

    /**
     * Returns array type.
     * @param componentType the component type
     * @return the created type
     */
    public static SqlCommon.TypeInfo array(@Nonnull SqlCommon.TypeInfo componentType) {
        Objects.requireNonNull(componentType);
        return array(componentType, 1);
    }

    /**
     * Returns array type.
     * @param aClass the component type, resolved by {@link #typeOf(Class)}
     * @param dimension the number of dimensions ({@code >= 1})
     * @return the created type
     * @see #column(String, Class)
     */
    public static SqlCommon.TypeInfo array(@Nonnull Class<?> aClass, int dimension) {
        Objects.requireNonNull(aClass);
        checkDimension(dimension);
        return array(typeOf(aClass), dimension);
    }

    /**
     * Returns array type.
     * @param componentType the component type
     * @param dimension the number of dimensions ({@code >= 1})
     * @return the created type
     */
    public static SqlCommon.TypeInfo array(
            @Nonnull SqlCommon.AtomType componentType,
            int dimension) {
        Objects.requireNonNull(componentType);
        checkDimension(dimension);
        return SqlCommon.TypeInfo.newBuilder()
                .setAtomType(componentType)
                .setDimension(dimension)
                .build();
    }

    /**
     * Returns array type.
     * @param componentType the component type
     * @param dimension the number of dimensions ({@code >= 1})
     * @return the created type
     */
    public static SqlCommon.TypeInfo array(
            @Nonnull SqlCommon.TypeInfo componentType,
            int dimension) {
        Objects.requireNonNull(componentType);
        checkDimension(dimension);
        return SqlCommon.TypeInfo.newBuilder(componentType)
                .setDimension(componentType.getDimension() + dimension)
                .build();
    }

    /**
     * Returns row type.
     * @param elements the row elements
     * @return the created type
     * @see #column(String, Class)
     */
    public static SqlCommon.TypeInfo row(@Nonnull SqlCommon.Column... elements) {
        Objects.requireNonNull(elements);
        return row(Arrays.asList(elements));
    }

    /**
     * Returns row type.
     * @param elements the row elements
     * @return the created type
     * @see #column(String, Class)
     */
    public static SqlCommon.TypeInfo row(@Nonnull List<? extends SqlCommon.Column> elements) {
        Objects.requireNonNull(elements);
        return SqlCommon.TypeInfo.newBuilder()
                .setRowType(SqlCommon.RowType.newBuilder()
                        .addAllColumns(elements))
                .build();
    }

    /**
     * Returns a column of rows.
     * @param name the optional column name
     * @param aClass the column type, resolved by {@link #typeOf(Class)}
     * @return the created column info
     * @throws IllegalArgumentException if there is no corresponding type
     */
    public static SqlCommon.Column column(@Nullable String name, @Nonnull Class<?> aClass) {
        Objects.requireNonNull(aClass);
        return column(name, typeOf(aClass));
    }

    /**
     * Returns a column of rows.
     * @param aClass the column type, resolved by {@link #typeOf(Class)}
     * @return the created column info
     * @throws IllegalArgumentException if there is no corresponding type
     */
    public static SqlCommon.Column column(@Nonnull Class<?> aClass) {
        Objects.requireNonNull(aClass);
        return column(typeOf(aClass));
    }

    /**
     * Returns a column of rows.
     * @param name the optional column name
     * @param type the column type
     * @return the created column info
     */
    public static SqlCommon.Column column(@Nullable String name, @Nonnull SqlCommon.AtomType type) {
        Objects.requireNonNull(type);
        var builder = SqlCommon.Column.newBuilder();
        if (Objects.nonNull(name)) {
            builder.setName(name);
        }
        builder.setAtomType(type);
        return builder.build();
    }

    /**
     * Returns a column of rows.
     * @param type the column type
     * @return the created column info
     */
    public static SqlCommon.Column column(@Nonnull SqlCommon.AtomType type) {
        Objects.requireNonNull(type);
        return column(null, type);
    }

    /**
     * Returns a column of rows.
     * @param name the optional column name
     * @param type the column type
     * @return the created column info
     */
    public static SqlCommon.Column column(@Nullable String name, @Nonnull SqlCommon.TypeInfo type) {
        Objects.requireNonNull(type);
        var builder = SqlCommon.Column.newBuilder();
        if (Objects.nonNull(name)) {
            builder.setName(name);
        }
        switch (type.getTypeInfoCase()) {
        case ATOM_TYPE:
            builder.setAtomType(type.getAtomType());
            break;
        case ROW_TYPE:
            builder.setRowType(type.getRowType());
            break;
        case USER_TYPE:
            builder.setUserType(type.getUserType());
            break;
        case TYPEINFO_NOT_SET:
            throw new IllegalArgumentException("type is not set");
        default:
            throw new IllegalArgumentException(MessageFormat.format(
                    "unrecognized type: {0}",
                    type.getTypeInfoCase()));
        }
        builder.setDimension(type.getDimension());
        return builder.build();
    }

    /**
     * Returns a column of rows.
     * @param type the column type
     * @return the created column info
     */
    public static SqlCommon.Column column(@Nonnull SqlCommon.TypeInfo type) {
        Objects.requireNonNull(type);
        return column(null, type);
    }

    /**
     * Returns user defined type {@literal (a.k.a. distinguished type)}.
     * @param name the type name
     * @return the created type
     */
    public static SqlCommon.TypeInfo user(@Nonnull String name) {
        Objects.requireNonNull(name);
        return SqlCommon.TypeInfo.newBuilder()
                .setUserType(SqlCommon.UserType.newBuilder()
                        .setName(name))
                .build();
    }

    private static void checkDimension(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "the number of dimensions must be >= 1: {0}",
                    dimension));
        }
    }

    private static SqlCommon.TypeInfo wrap(@Nonnull SqlCommon.AtomType kind) {
        assert kind != null;
        return SqlCommon.TypeInfo.newBuilder()
                .setAtomType(kind)
                .build();
    }
}
