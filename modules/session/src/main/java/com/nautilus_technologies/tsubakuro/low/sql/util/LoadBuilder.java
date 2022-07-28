package com.nautilus_technologies.tsubakuro.low.sql.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.TableMetadata;
import com.nautilus_technologies.tsubakuro.low.sql.Types;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.MappedFutureResponse;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;

/**
 * Builds {@link Load} operations.
 * @see Load
 * @see #loadTo(TableMetadata)
 */
public class LoadBuilder {

    private static final int MAX_PLACEHOLDER_NAME_SUFFIX_LENGTH = 10;

    private static final String FORMAT_PLACEHOLDER_NAME_SHORT = "p%d"; //$NON-NLS-1$

    private static final String FORMAT_PLACEHOLDER_NAME_LONG = "p%d_%s"; //$NON-NLS-1$

    private static final Pattern PATTERN_REGULAR_IDENTIFIER = Pattern.compile("[A-Za-z_][0-9A-Za-z_]*"); //$NON-NLS-1$

    private static final Style DEFAULT_STYLE = Style.ERROR;

    private static final Map<SqlCommon.AtomType, String> ATOM_TYPE_NAMES = Map.ofEntries(
            Map.entry(SqlCommon.AtomType.BIT, "VARBIT"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.BOOLEAN, "BOOLEAN"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.CHARACTER, "VARCHAR"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.DATE, "DATE"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.DATETIME_INTERVAL, "DATETIME_INTERVAL"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.DECIMAL, "DECIMAL"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.FLOAT4, "FLOAT"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.FLOAT8, "DOUBLE"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.INT4, "INT"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.INT8, "BIGINT"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.OCTET, "VAROCTET"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.TIME_OF_DAY, "TIME"), //$NON-NLS-1$
            Map.entry(SqlCommon.AtomType.TIME_POINT, "TIME_POINT")); //$NON-NLS-1$

    private final TableMetadata destination;

    private final List<Entry> entries = new ArrayList<>();

    private Style style = DEFAULT_STYLE;

    /**
     * Represents a kind of behavior when a row with the same primary key is already exists on the destination table.
     */
    public enum Style {

        /**
         * Raises an error if the primary key is conflicted.
         */
        ERROR,

        /**
         * Skips the row if the primary key is conflicted.
         */
        SKIP,

        /**
         * Overwrites the row if the primary key is conflicted.
         */
        OVERWRITE,
    }

    /**
     * Creates a new instance.
     * @param destination the destination table information
     */
    public LoadBuilder(@Nonnull TableMetadata destination) {
        Objects.requireNonNull(destination);
        this.destination = destination;
    }

    /**
     * Creates a new instance.
     * @param destination the destination table information
     * @return the created instance.
     */
    public static LoadBuilder loadTo(@Nonnull TableMetadata destination) {
        Objects.requireNonNull(destination);
        return new LoadBuilder(destination);
    }

    /**
     * Declares behavior if a row with the same primary key is already exists on the destination table.
     * @param newStyle the behavior type
     * @return this
     */
    public LoadBuilder style(@Nonnull Style newStyle) {
        Objects.requireNonNull(newStyle);
        this.style = newStyle;
        return this;
    }

    /**
     * Declares to raise error if the primary key is conflict.
     * @return this
     * @see Style#ERROR
     */
    public LoadBuilder errorOnCoflict() {
        return style(Style.ERROR);
    }

    /**
     * Declares to skip rows if the primary key is conflict.
     * @return this
     * @see Style#SKIP
     */
    public LoadBuilder skipOnCoflict() {
        return style(Style.SKIP);
    }

    /**
     * Declares to overwrite rows if the primary key is conflict.
     * @return this
     * @see Style#OVERWRITE
     */
    public LoadBuilder overwriteOnCoflict() {
        return style(Style.OVERWRITE);
    }

    /**
     * Declares a column mapping between the destination and source column.
     * <p>
     * Note that, if the column types are inconsistent between destination and source columns,
     * the load operation will automatically converts data type of input values into the destination column type.
     * Or the conversion would be failure, the load operation also will be failure.
     * </p>
     * @param destinationColumn the destination table column
     * @param sourceColumnName the source column on input files
     * @param sourceType the source column type, resolved by {@link Types#typeOf(Class) Types.typeOf(aClass)}.
     * @return this
     */
    public LoadBuilder mapping(
            @Nonnull SqlCommon.Column destinationColumn,
            @Nonnull String sourceColumnName,
            @Nonnull Class<?> sourceType) {
        Objects.requireNonNull(destinationColumn);
        Objects.requireNonNull(sourceColumnName);
        Objects.requireNonNull(sourceType);
        String name = createPlaceHolderName(destinationColumn, entries.size());
        this.entries.add(new Entry(
                destinationColumn,
                Placeholders.of(name, sourceType),
                Parameters.referenceColumn(name, sourceColumnName)));
        return this;
    }

    /**
     * Declares a column mapping between the destination and source column.
     * <p>
     * Note that, if the column types are inconsistent between destination and source columns,
     * the load operation will automatically converts data type of input values into the destination column type.
     * Or the conversion would be failure, the load operation also will be failure.
     * </p>
     * @param destinationColumn the destination table column
     * @param sourceColumnName the source column on input files
     * @param sourceType the source column type
     * @return this
     * @throws IllegalArgumentException if the destination column information is not valid
     */
    public LoadBuilder mapping(
            @Nonnull SqlCommon.Column destinationColumn,
            @Nonnull String sourceColumnName,
            @Nonnull SqlCommon.AtomType sourceType) {
        Objects.requireNonNull(destinationColumn);
        Objects.requireNonNull(sourceColumnName);
        Objects.requireNonNull(sourceType);
        String name = createPlaceHolderName(destinationColumn, entries.size());
        this.entries.add(new Entry(
                destinationColumn,
                Placeholders.of(name, sourceType),
                Parameters.referenceColumn(name, sourceColumnName)));
        return this;
    }

    /**
     * Declares a column mapping between the destination and source column.
     * <p>
     * Note that, if the column types are inconsistent between destination and source columns,
     * the load operation will automatically converts data type of input values into the destination column type.
     * Or the conversion would be failure, the load operation also will be failure.
     * </p>
     * @param destinationColumn the destination table column
     * @param sourceColumnPosition the source column position (0-origin)
     * @param sourceType the source column type, resolved by {@link Types#typeOf(Class) Types.typeOf(aClass)}.
     * @return this
     * @throws IllegalArgumentException if the destination column information is not valid
     */
    public LoadBuilder mapping(
            @Nonnull SqlCommon.Column destinationColumn,
            int sourceColumnPosition,
            @Nonnull Class<?> sourceType) {
        Objects.requireNonNull(destinationColumn);
        Objects.requireNonNull(sourceType);
        String name = createPlaceHolderName(destinationColumn, entries.size());
        this.entries.add(new Entry(
                destinationColumn,
                Placeholders.of(name, sourceType),
                Parameters.referenceColumn(name, sourceColumnPosition)));
        return this;
    }

    /**
     * Declares a column mapping between the destination and source column.
     * <p>
     * Note that, if the column types are inconsistent between destination and source columns,
     * the load operation will automatically converts data type of input values into the destination column type.
     * Or the conversion would be failure, the load operation also will be failure.
     * </p>
     * @param destinationColumn the destination table column
     * @param sourceColumnPosition the source column position (0-origin)
     * @param sourceType the source column type
     * @return this
     * @throws IllegalArgumentException if the destination column information is not valid
     */
    public LoadBuilder mapping(
            @Nonnull SqlCommon.Column destinationColumn,
            int sourceColumnPosition,
            @Nonnull SqlCommon.AtomType sourceType) {
        Objects.requireNonNull(destinationColumn);
        Objects.requireNonNull(sourceType);
        String name = createPlaceHolderName(destinationColumn, entries.size());
        this.entries.add(new Entry(
                destinationColumn,
                Placeholders.of(name, sourceType),
                Parameters.referenceColumn(name, sourceColumnPosition)));
        return this;
    }

    /**
     * Declares a column mapping between the destination and source column.
     * <p>
     * This assumes the source column type is compatible to the destination column type.
     * If the source column type is no such the type, the load operation will be failure.
     * </p>
     * @param destinationColumn the destination table column
     * @param sourceColumnName the source column on input files
     * @return this
     * @throws IllegalArgumentException if the destination column information is not valid
     */
    public LoadBuilder mapping(
            @Nonnull SqlCommon.Column destinationColumn,
            @Nonnull String sourceColumnName) {
        Objects.requireNonNull(destinationColumn);
        Objects.requireNonNull(sourceColumnName);
        String name = createPlaceHolderName(destinationColumn, entries.size());
        this.entries.add(new Entry(
                destinationColumn,
                createPlaceHolder(name, destinationColumn),
                Parameters.referenceColumn(name, sourceColumnName)));
        return this;
    }

    /**
     * Declares a column mapping between the destination and source column.
     * <p>
     * This assumes the source column type is compatible to the destination column type.
     * If the source column type is no such the type, the load operation will be failure.
     * </p>
     * @param destinationColumn the destination table column
     * @param sourceColumnPosition the source column position (0-origin)
     * @return this
     * @throws IllegalArgumentException if the destination column information is not valid
     */
    public LoadBuilder mapping(
            @Nonnull SqlCommon.Column destinationColumn,
            @Nonnull int sourceColumnPosition) {
        Objects.requireNonNull(destinationColumn);
        String name = createPlaceHolderName(destinationColumn, entries.size());
        this.entries.add(new Entry(
                destinationColumn,
                createPlaceHolder(name, destinationColumn),
                Parameters.referenceColumn(name, sourceColumnPosition)));
        return this;
    }

    private static String createPlaceHolderName(@Nonnull SqlCommon.Column destinationColumn, int index) {
        assert destinationColumn != null;
        var columnName = destinationColumn.getName();
        if (columnName.length() > 10) {
            columnName = columnName.substring(0, MAX_PLACEHOLDER_NAME_SUFFIX_LENGTH);
        }
        if (PATTERN_REGULAR_IDENTIFIER.matcher(columnName).matches()) {
            return String.format(FORMAT_PLACEHOLDER_NAME_LONG, index, columnName);
        }
        return String.format(FORMAT_PLACEHOLDER_NAME_SHORT, index);
    }

    private static SqlRequest.PlaceHolder createPlaceHolder(
            @Nonnull String name,
            @Nonnull SqlCommon.Column destinationColumn) {
        assert name != null;
        assert destinationColumn != null;
        var result = SqlRequest.PlaceHolder.newBuilder()
                .setName(name)
                .setDimension(destinationColumn.getDimension());
        switch (destinationColumn.getTypeInfoCase()) {
        case TYPEINFO_NOT_SET:
            return result.build();
        case ATOM_TYPE:
            return result.setAtomType(destinationColumn.getAtomType()).build();
        case ROW_TYPE:
            return result.setRowType(destinationColumn.getRowType()).build();
        case USER_TYPE:
            return result.setUserType(destinationColumn.getUserType()).build();
        default:
            throw new AssertionError();
        }
    }

    /**
     * Builds a {@link Load} operation executor.
     * <p>
     * The created object will be disposed after the {@code client} is disposed.
     * </p>
     * @param client the corresponding SQL client
     * @return the future response of the executor
     * @throws IOException if I/O error was occurred while creating the executor
     * @throws IllegalStateException if there are not column mappings
     */
    public FutureResponse<Load> build(@Nonnull SqlClient client) throws IOException {
        Objects.requireNonNull(client);
        if (entries.isEmpty()) {
            throw new IllegalStateException("there are not column mappings to load");
        }
        var query = buildQuery();
        var placeholders = entries.stream()
                .map(it -> it.from)
                .collect(Collectors.toList());
        var parameters = entries.stream()
                .map(it -> it.to)
                .collect(Collectors.toList());
        return new MappedFutureResponse<>(
                client.prepare(query, placeholders),
                it -> new Load(destination, it, parameters));
    }

    private String buildQuery() {
        // INSERT
        StringBuilder buf = new StringBuilder();
        switch (style) {
        case ERROR:
            buf.append("INSERT"); //$NON-NLS-1$
            break;
        case OVERWRITE:
            buf.append("UPDATE OR INSERT"); //$NON-NLS-1$
            break;
        case SKIP:
            buf.append("INSERT IF NOT EXISTS"); //$NON-NLS-1$
            break;
        default:
            throw new AssertionError();
        }
        buf.append(' ');

        // INTO
        buf.append("INTO");
        buf.append(' ');

        // table
        if (destination.getSchemaName().isPresent()) {
            if (destination.getDatabaseName().isPresent()) {
                buf.append(identifier(destination.getDatabaseName().get()));
                buf.append('.');
            }
            buf.append(identifier(destination.getSchemaName().get()));
            buf.append('.');
        }
        buf.append(identifier(destination.getTableName()));
        buf.append(' ');

        // (...)
        assert !entries.isEmpty();
        buf.append('(');
        {
            boolean cont = false;
            for (var entry : entries) {
                if (cont) {
                    buf.append(',');
                    buf.append(' ');
                }
                cont = true;
                buf.append(identifier(entry.column.getName()));
            }
        }
        buf.append(')');
        buf.append(' ');

        // VALUES (
        buf.append("VALUES"); //$NON-NLS-1$
        buf.append(' ');
        buf.append('(');
        {
            boolean cont = false;
            for (var entry : entries) {
                if (cont) {
                    buf.append(',');
                    buf.append(' ');
                }
                cont = true;
                buf.append(buildSourceValue(entry));
            }
        }
        buf.append(')');

        return buf.toString();
    }

    private static String identifier(@Nonnull String name) {
        assert name != null;
        if (PATTERN_REGULAR_IDENTIFIER.matcher(name).matches()) {
            return name;
        }
        var buf = new StringBuilder(name.length() + 2);
        buf.append('"');
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            if (c == '"') {
                buf.append('"');
            }
            buf.append(c);
        }
        buf.append('"');
        return buf.toString();
    }

    private String buildSourceValue(@Nonnull Entry entry) {
        assert entry != null;
        if (entry.hasCompatibleType()) {
            return String.format(":%s", entry.from.getName()); //$NON-NLS-1$
        }
        var buf = new StringBuilder();
        buf.append("CAST(:"); //$NON-NLS-1$
        buf.append(entry.from.getName());
        buf.append(" AS "); //$NON-NLS-1$
        buildTypeName(entry.column, buf);
        buf.append(')');
        return buf.toString();
    }

    private void buildTypeName(@Nonnull SqlCommon.Column column, @Nonnull StringBuilder buf) {
        assert column != null;
        assert buf != null;
        switch (column.getTypeInfoCase()) {
        case TYPEINFO_NOT_SET:
            throw new IllegalStateException(MessageFormat.format(
                    "column type is not set: {0}.{1}",
                    destination.getTableName(),
                    column.getName()));
        case ATOM_TYPE: {
                var name = ATOM_TYPE_NAMES.get(column.getAtomType());
                if (name == null) {
                    throw new IllegalStateException(MessageFormat.format(
                            "unsupported column type: {0}",
                            column.getAtomType()));
                }
                buf.append(name);
                break;
            }
        case ROW_TYPE:
            buf.append("ROW("); //$NON-NLS-1$
            {
                boolean cont = false;
                for (var element : column.getRowType().getColumnsList()) {
                    if (cont) {
                        buf.append(", "); //$NON-NLS-1$
                    }
                    cont = true;
                    buildTypeName(element, buf);
                }
            }
            buf.append(')');
            break;
        case USER_TYPE:
            // FIXME: qualifier
            buf.append(identifier(column.getUserType().getName()));
            break;
        default:
            throw new AssertionError();
        }
    }

    private static class Entry {

        final SqlCommon.Column column;

        final SqlRequest.PlaceHolder from;

        final SqlRequest.Parameter to;

        Entry(
                @Nonnull SqlCommon.Column column,
                @Nonnull SqlRequest.PlaceHolder from,
                @Nonnull SqlRequest.Parameter to) {
            assert column != null;
            assert from != null;
            assert to != null;
            this.column = column;
            this.from = from;
            this.to = to;
        }

        boolean hasCompatibleType() {
            if (column.getDimension() != from.getDimension()) {
                return false;
            }
            switch (column.getTypeInfoCase()) {
            case ATOM_TYPE:
                if (from.getTypeInfoCase() != SqlRequest.PlaceHolder.TypeInfoCase.ATOM_TYPE) {
                    return false;
                }
                break;
            case ROW_TYPE:
                if (from.getTypeInfoCase() != SqlRequest.PlaceHolder.TypeInfoCase.ROW_TYPE) {
                    return false;
                }
                break;
            case USER_TYPE:
                if (from.getTypeInfoCase() != SqlRequest.PlaceHolder.TypeInfoCase.USER_TYPE) {
                    return false;
                }
                break;
            default:
                return false;
            }
            switch (column.getTypeInfoCase()) {
            case TYPEINFO_NOT_SET:
                return true;
            case ATOM_TYPE:
                return column.getAtomType() == from.getAtomType();
            case ROW_TYPE:
                return Objects.equals(column.getRowType(), column.getRowType());
            case USER_TYPE:
                return Objects.equals(column.getUserType(), column.getUserType());
            default:
                throw new AssertionError();
            }
        }
    }
}
