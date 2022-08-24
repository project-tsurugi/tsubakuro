package  com.nautilus_technologies.tsubakuro.impl.low.sql.testing;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// import javax.annotation.Nonnull;
// import javax.annotation.Nullable;

import com.nautilus_technologies.tsubakuro.low.sql.RelationCursor;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetMetadata;
import com.nautilus_technologies.tsubakuro.impl.low.sql.BasicResultSet;
import com.nautilus_technologies.tsubakuro.impl.low.sql.ValueInputBackedRelationCursor;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.nautilus_technologies.tsubakuro.low.sql.io.StreamBackedValueInput;
import com.nautilus_technologies.tsubakuro.low.sql.io.StreamBackedValueOutput;
import com.nautilus_technologies.tsubakuro.low.sql.io.ValueInput;

/**
 * Represents a relation data.
 *
 * <h3 id="value-mapping"> Value mapping </h3>
 * <table>
 *   <thead>
 *     <tr>
 *       <th> Java type </th>
 *       <th> SQL type </th>
 *     </tr>
 *   </thead>
 *   <tbody>
 *     <tr>
 *       <td> {@link Boolean} </td>
 *       <td> {@link RelationCursor#fetchBooleanValue() BOOLEAN} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link Integer} </td>
 *       <td> {@link RelationCursor#fetchInt4Value() INT4} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link Long} </td>
 *       <td> {@link RelationCursor#fetchInt8Value() INT8} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link Float} </td>
 *       <td> {@link RelationCursor#fetchFloat4Value() FLOAT4} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link Double} </td>
 *       <td> {@link RelationCursor#fetchFloat8Value() FLOAT8} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link BigDecimal} </td>
 *       <td> {@link RelationCursor#fetchDecimalValue() DECIMAL} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link String} </td>
 *       <td> {@link RelationCursor#fetchCharacterValue() CHARACTER} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code byte[]} </td>
 *       <td> {@link RelationCursor#fetchOctetValue() OCTET} </td>
 *     </tr>
 *     <tr>
 *       <td> {@code boolean[]} </td>
 *       <td> {@link RelationCursor#fetchBitValue() BIT} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link java.time.LocalDate} </td>
 *       <td> {@link RelationCursor#fetchDateValue() DATE} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link java.time.LocalTime} </td>
 *       <td> {@link RelationCursor#fetchTimeOfDayValue() TIME_OF_DAY} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link java.time.Instant} </td>
 *       <td> {@link RelationCursor#fetchTimePointValue() TIME_POINT} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link DateTimeInterval} </td>
 *       <td> {@link RelationCursor#fetchDateTimeIntervalValue() DATE_TIME_INTERVAL} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link Row} (by {@link #row(List) row()})</td>
 *       <td> {@link RelationCursor#beginRowValue() ROW} </td>
 *     </tr>
 *     <tr>
 *       <td> {@link Array} (by {@link #array(List) array()})</td>
 *       <td> {@link RelationCursor#beginArrayValue() ARRAY} </td>
 *     </tr>
 *     <tr>
 *       <td> ({@code null} value) </td>
 *       <td> ({@link RelationCursor#isNull() NULL}) </td>
 *     </tr>
 *   </tbody>
 * </table>
 */
public final class Relation {

    /**
     * Represents a {@code NULL} to avoid accidental method override resolution.
     */
    public static final Value NULL = convert(null);

    private final List<? extends Entry> entries;

    private Relation(List<? extends Entry> entries) {
        Objects.requireNonNull(entries);
        this.entries = entries;
    }

    /**
     * Returns byte sequence of this relation.
     * {@link StreamBackedValueInput} can extract the contents.
     * @return the byte sequence.
     */
    public ByteBuffer getByteBuffer() {
        return ByteBuffer.wrap(getBytes());
    }

    /**
     * Returns byte sequence of this relation.
     * {@link StreamBackedValueInput} can extract the contents.
     * @return the byte sequence.
     */
    public byte[] getBytes() {
        try (
            var buf = new ByteArrayOutputStream();
            var output = new StreamBackedValueOutput(buf);
        ) {
            for (var entry : entries) {
                switch (entry.getType()) {
                case NULL:
                    output.writeNull();
                    break;
                case INT:
                    output.writeInt(entry.getIntValue());
                    break;
                case FLOAT4:
                    output.writeFloat4(entry.getFloat4Value());
                    break;
                case FLOAT8:
                    output.writeFloat8(entry.getFloat8Value());
                    break;
                case DECIMAL:
                    output.writeDecimal(entry.getDecimalValue());
                    break;
                case CHARACTER:
                    output.writeCharacter(entry.getCharacterValue());
                    break;
                case OCTET:
                    output.writeOctet(entry.getOctetValue());
                    break;
                case BIT:
                    output.writeBit(entry.getBitValue());
                    break;
                case DATE:
                    output.writeDate(entry.getDateValue());
                    break;
                case TIME_OF_DAY:
                    output.writeTimeOfDay(entry.getTimeOfDayValue());
                    break;
                case TIME_POINT:
                    output.writeTimePoint(entry.getTimePointValue());
                    break;
                case DATETIME_INTERVAL:
                    output.writeDateTimeInterval(entry.getDateTimeIntervalValue());
                    break;
                case ROW:
                    output.writeRowBegin(entry.getRowSize());
                    break;
                case ARRAY:
                    output.writeArrayBegin(entry.getArraySize());
                    break;
                case END_OF_CONTENTS:
                    output.writeEndOfContents();
                    break;

                case CLOB:
                case BLOB:
                    // FIXME impl
                default:
                    throw new UnsupportedOperationException(entry.toString());
                }
            }
            output.flush();
            return buf.toByteArray();
        } catch (Exception e) {
            // may not occur
            throw new AssertionError(e);
        }
    }

    /**
     * Returns a new {@link ValueInput} to retrieve contents of this relation.
     * @return the {@link ValueInput} to retrieve contents
     */
    public ValueInput getValueInput() {
        return new EntrySequenceValueInput(entries);
    }

    /**
     * Returns a new {@link ResultSet} to retrieve contents of this relation.
     * @param metadata the metadata of the result set
     * @return the {@link ResultSet} to retrieve contents
     */
    public ResultSet getResultSet(ResultSetMetadata metadata) {
        Objects.requireNonNull(metadata);
        return new BasicResultSet(metadata, new ValueInputBackedRelationCursor(getValueInput()));
    }

    /**
     * Creates a new relation from a 2-dimensional value array.
     * <p>
     * The first dimension represents individual elements of rows.
     * This method is equivalent to the following:
<pre>
// converts each elements into rows
List<Row> rows = of(Arrays.stream(values)
    .map(it -> row(it)
    .collect(Collectors.toList());
// builds a relation
return of(rows);
</pre>
     *
     * </p>
     * <p>
     * Individual elements must be convertible to SQL values.
     * Please see the <a href="#value-mapping">Value mapping section</a>.
     * </p>
     * @param values individual values in the relation
     * @return the created relation
     * @throws IllegalArgumentException if some values are not convertible
     */
    public static Relation of(Object[][] values) {
        Objects.requireNonNull(values);
        return of(Arrays.stream(values)
                .map(it -> row(it))
                .collect(Collectors.toList()));
    }

    /**
     * Creates a new relation from array of {@link Row}s.
     * @param rows array of rows composing the relation
     * @return the created relation
     */
    public static Relation of(Row... rows) {
        Objects.requireNonNull(rows);
        return of(Arrays.asList(rows));
    }

    /**
     * Creates a new relation from list of {@link Row}s.
     * @param rows list of rows composing the relation
     * @return the created relation
     */
    public static Relation of(List<? extends Row> rows) {
        Objects.requireNonNull(rows);
        var entries = new ArrayList<Entry>();
        rows.forEach(it -> it.dumpTo(entries::add));
        return new Relation(entries);
    }

    /**
     * Builds a {@link Row} from element values.
     * <p>
     * Individual elements must be convertible to SQL values.
     * Please see the <a href="#value-mapping">Value mapping section</a>.
     * </p>
     * @param elements the element value of this row
     * @return the created row
     * @throws IllegalArgumentException if some values are not convertible
     */
    public static Row row(List<?> elements) {
        Objects.requireNonNull(elements);
        return row(elements.stream());
    }

    /**
     * Builds a {@link Row} from element values.
     * <p>
     * Individual elements must be convertible to SQL values.
     * Please see the <a href="#value-mapping">Value mapping section</a>.
     * </p>
     * <p>
     * Please consider to use {@link Relation#NULL} instead of {@code null} value,
     * to avoid accidental method overload resolution caused by {@code null}.
     * For example, {@code row(null)} is evaluated as {@code row((Object[]) null)}.
     * </p>
     * @param elements the element value of this row
     * @return the created row
     * @throws IllegalArgumentException if some values are not convertible
     * @see #row(List)
     */
    public static Row row(Object... elements) {
        Objects.requireNonNull(elements);
        return row(Arrays.stream(elements));
    }

    /**
     * Returns an empty {@link Row}.
     * @return the empty row
     * @see #row(List)
     */
    public static Row row() {
        return row(Stream.empty());
    }

    /**
     * Builds a {@link Array} from element values.
     * <p>
     * Individual elements must be convertible to SQL values.
     * Please see the <a href="#value-mapping">Value mapping section</a>.
     * </p>
     * @param elements the element value of this array
     * @return the created array
     * @throws IllegalArgumentException if some values are not convertible
     */
    public static Array array(List<?> elements) {
        Objects.requireNonNull(elements);
        return array(elements.stream());
    }

    /**
     * Builds a {@link Array} from element values.
     * <p>
     * Individual elements must be convertible to SQL values.
     * Please see the <a href="#value-mapping">Value mapping section</a>.
     * </p>
     * <p>
     * Please consider to use {@link Relation#NULL} instead of {@code null} value,
     * to avoid accidental method overload resolution caused by {@code null}.
     * For example, {@code array(null)} is evaluated as {@code array((Object[]) null)}.
     * </p>
     * @param elements the element value of this array
     * @return the created array
     * @throws IllegalArgumentException if some values are not convertible
     * @see #array(List)
     */
    public static Array array(Object... elements) {
        Objects.requireNonNull(elements);
        return array(Arrays.stream(elements));
    }

    /**
     * Returns an empty {@link Array}.
     * @return the empty array
     * @see #array(List)
     */
    public static Array array() {
        return array(Stream.empty());
    }

    private static Row row(Stream<?> elements) {
        Objects.requireNonNull(elements);
        return new Row(elements
                .map(Relation::convert)
                .collect(Collectors.toList()));
    }

    private static Array array(Stream<?> elements) {
        Objects.requireNonNull(elements);
        return new Array(elements
                .map(Relation::convert)
                .collect(Collectors.toList()));
    }

    private static Value convert(Object value) {
        if (value instanceof Value) {
            return (Value) value;
        }

        Optional<Entry> entry = Entry.parse(value);
        if (entry.isPresent()) {
            return new Atom(entry.get());
        }

        throw new IllegalArgumentException(MessageFormat.format(
                "unrecognized value: {0}",
                value));
    }

    private abstract static class Value {
        abstract void dumpTo(Consumer<? super Entry> output);
    }

    private static final class Atom extends Value {

        private final Entry entry;

        Atom(Entry entry) {
            assert entry != null;
            this.entry = entry;
        }

        @Override
        void dumpTo(Consumer<? super Entry> output) {
            assert output != null;
            output.accept(entry);
        }
    }

    /**
     * Represents a row of relations or a row type value.
     */
    public static final class Row extends Value {

        private final List<? extends Value> elements;

        Row(List<? extends Value> values) {
            assert values != null;
            this.elements = values;
        }

        @Override
        void dumpTo(Consumer<? super Entry> output) {
            assert output != null;
            output.accept(Entry.forRow(elements.size()));
            elements.forEach(it -> it.dumpTo(output));
        }
    }

    /**
     * Represents an array type value.
     */
    public static final class Array extends Value {

        private final List<? extends Value> elements;

        Array(List<? extends Value> values) {
            assert values != null;
            this.elements = values;
        }

        @Override
        void dumpTo(Consumer<? super Entry> output) {
            assert output != null;
            output.accept(Entry.forArray(elements.size()));
            elements.forEach(it -> it.dumpTo(output));
        }
    }
}
