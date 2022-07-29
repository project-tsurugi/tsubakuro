package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.low.sql.RelationCursor;
import com.nautilus_technologies.tsubakuro.low.sql.io.BrokenRelationException;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.nautilus_technologies.tsubakuro.low.sql.io.EntryType;
import com.nautilus_technologies.tsubakuro.low.sql.io.ValueInput;

/**
 * An implementation of {@link RelationCursor}, which retrieves relation data from the {@link ValueInput}.
 */
public class ValueInputBackedRelationCursor implements RelationCursor {

    private static final EnumSet<EntryType> TOPLEVEL_EXPECTED_TYPES =
            EnumSet.of(EntryType.ROW, EntryType.END_OF_CONTENTS);

    private static final Set<EntryType> DECIMAL_EXPECTED_TYPES =
            EnumSet.of(EntryType.DECIMAL, EntryType.INT);

    static final Logger LOG = LoggerFactory.getLogger(ValueInputBackedRelationCursor.class);

    private final ValueInput input;

    private final Stack stack = new Stack();

    private EntryType currentColumnType = null;

    /**
     * Creates a new instance.
     * @param input the source input
     */
    public ValueInputBackedRelationCursor(@Nonnull ValueInput input) {
        Objects.requireNonNull(input);
        this.input = input;
    }

    @Override
    public boolean nextRow() throws IOException, InterruptedException {
        discardTopLevelRow();
        EntryType type = input.peekType();
        if (type == EntryType.END_OF_CONTENTS) {
            return false;
        } else if (type == EntryType.ROW) {
            int elements = input.readRowBegin();
            stack.push(EntryKind.TOP_LEVEL_ROW, elements);
            return true;
        } else {
            throw BrokenRelationException.sawUnexpectedValueType(type, TOPLEVEL_EXPECTED_TYPES);
        }
    }

    private void discardTopLevelRow() throws IOException, InterruptedException {
        while (!stack.isEmpty()) {
            discardCurrentFrame();
        }
    }

    private void discardCurrentFrame() throws IOException, InterruptedException {
        var entry = stack.getTop();
        for (int i = 0; i < entry.rest; i++) {
            forceDiscardCurrentEntry();
        }
        stack.pop();
        currentColumnType = null;
    }

    private void forceDiscardCurrentEntry() throws IOException, InterruptedException, BrokenRelationException {
        if (!input.skip(true)) {
            throw BrokenRelationException.sawUnexpectedEndOfContents();
        }
    }

    @Override
    public boolean nextColumn() throws IOException, InterruptedException {
        // not in top-level rows
        if (stack.isEmpty()) {
            return false;
        }

        // discard un-fetched entry
        discardCurrentColumnIfExists();

        // return false if all columns are consumed in this sequence
        assert !stack.isEmpty();
        if (stack.getTop().rest == 0) {
            return false;
        }

        assert currentColumnType == null;
        currentColumnType = input.peekType();
        if (currentColumnType == EntryType.END_OF_CONTENTS) {
            throw BrokenRelationException.sawUnexpectedEndOfContents();
        }
        return true;
    }

    private void discardCurrentColumnIfExists() throws IOException, InterruptedException {
        assert stack.isEmpty() == false;
        if (currentColumnType != null) {
            forceDiscardCurrentEntry();
            columnConsumed();
        }
    }

    private void columnConsumed() {
        currentColumnType = null;
        var entry = stack.getTop();
        assert entry.rest > 0;
        entry.rest--;
    }

    @Override
    public boolean isNull() {
        var type = checkColumnPrepared();

        // NOTE: never fetch the value to retrieve non-null values
        return type == EntryType.NULL;
    }

    @Override
    public boolean fetchBooleanValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.INT);
        long intValue = input.readInt();
        columnConsumed();
        if (intValue == 0) {
            return false;
        } else if (intValue == 1) {
            return true;
        }
        throw BrokenRelationException.sawIntValueOutOfRange(boolean.class, intValue);
    }

    @Override
    public int fetchInt4Value() throws IOException, InterruptedException {
        requireColumnType(EntryType.INT);
        long intValue = input.readInt();
        columnConsumed();
        if (Integer.MIN_VALUE <= intValue && intValue <= Integer.MAX_VALUE) {
            return (int) intValue;
        }
        throw BrokenRelationException.sawIntValueOutOfRange(int.class, intValue);
    }

    @Override
    public long fetchInt8Value() throws IOException, InterruptedException {
        requireColumnType(EntryType.INT);
        long intValue = input.readInt();
        columnConsumed();
        return intValue;
    }

    @Override
    public float fetchFloat4Value() throws IOException, InterruptedException {
        requireColumnType(EntryType.FLOAT4);
        var value = input.readFloat4();
        columnConsumed();
        return value;
    }

    @Override
    public double fetchFloat8Value() throws IOException, InterruptedException {
        requireColumnType(EntryType.FLOAT8);
        var value = input.readFloat8();
        columnConsumed();
        return value;
    }

    @Override
    public BigDecimal fetchDecimalValue() throws IOException, InterruptedException {
        requireColumnType(DECIMAL_EXPECTED_TYPES);
        var value = input.readDecimal();
        columnConsumed();
        return value;
    }

    @Override
    public String fetchCharacterValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.CHARACTER);
        var value = input.readCharacter();
        columnConsumed();
        return value;
    }

    @Override
    public byte[] fetchOctetValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.OCTET);
        var value = input.readOctet();
        columnConsumed();
        return value;
    }

    @Override
    public boolean[] fetchBitValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.BIT);
        var value = input.readBit();
        columnConsumed();
        return value;
    }

    @Override
    public LocalDate fetchDateValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.DATE);
        var value = input.readDate();
        columnConsumed();
        return value;
    }

    @Override
    public LocalTime fetchTimeOfDayValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.TIME_OF_DAY);
        var value = input.readTimeOfDay();
        columnConsumed();
        return value;
    }

    @Override
    public Instant fetchTimePointValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.TIME_POINT);
        var value = input.readTimePoint();
        columnConsumed();
        return value;
    }

    @Override
    public DateTimeInterval fetchDateTimeIntervalValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.DATETIME_INTERVAL);
        var value = input.readDateTimeInterval();
        columnConsumed();
        return value;
    }

    @Override
    public int beginArrayValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.ARRAY);
        int count = input.readArrayBegin();
        columnConsumed();

        stack.push(EntryKind.ARRAY_VALUE, count);
        return count;
    }

    @Override
    public void endArrayValue() throws IOException, InterruptedException {
        Entry entry = stack.getTop();
        if (entry.kind != EntryKind.ARRAY_VALUE) {
            throw new IllegalStateException("not in array value");
        }
        discardCurrentFrame();
    }

    @Override
    public int beginRowValue() throws IOException, InterruptedException {
        requireColumnType(EntryType.ROW);
        int count = input.readRowBegin();
        columnConsumed();

        stack.push(EntryKind.ROW_VALUE, count);
        return count;
    }

    @Override
    public void endRowValue() throws IOException, InterruptedException {
        Entry entry = stack.getTop();
        if (entry.kind != EntryKind.ROW_VALUE) {
            throw new IllegalStateException("not in row value");
        }
        discardCurrentFrame();
    }

    @Override
    public void close() throws IOException, InterruptedException {
        input.close();
    }

    private EntryType checkColumnPrepared() {
        EntryType type = currentColumnType;
        if (type == null) {
            throw new IllegalStateException("invoke .nextColumn() before fetch value");
        }
        return type;
    }

    private void requireColumnType(@Nonnull EntryType expected) throws BrokenRelationException {
        assert expected != null;
        var found = checkColumnPrepared();
        if (found != expected) {
            throw BrokenRelationException.sawUnexpectedValueType(found, expected);
        }
    }

    private void requireColumnType(@Nonnull Set<EntryType> expected) throws BrokenRelationException {
        assert expected != null;
        var found = checkColumnPrepared();
        if (!expected.contains(found)) {
            throw BrokenRelationException.sawUnexpectedValueType(found, expected);
        }
    }

    private enum EntryKind {
        TOP_LEVEL_ROW,
        ROW_VALUE,
        ARRAY_VALUE,
    }

    private static final class Entry {
        EntryKind kind;
        int rest;
    }

    private static final class Stack {

        private static final Entry[] EMPTY = new Entry[0];

        private Entry[] elements = EMPTY;

        private int size = 0;

        Stack() {
            super();
        }

        public void push(EntryKind kind, int rest) {
            assert kind != null;
            assert rest >= 0;

            if (elements.length <= size) {
                int oldCapacity = elements.length;
                elements = Arrays.copyOf(elements, Math.max(oldCapacity * 2, 4));
                for (int i = oldCapacity; i < elements.length; i++) {
                    elements[i] = new Entry();
                }
            }
            size++;
            Entry top = getTop();
            top.kind = kind;
            top.rest = rest;
        }

        public void pop() {
            checkNotEmpty();
            size--;
        }

        public boolean isEmpty() {
            return size == 0;
        }

        public Entry getTop() {
            checkNotEmpty();
            return elements[size - 1];
        }

        private void checkNotEmpty() {
            if (isEmpty()) {
                throw new IllegalStateException();
            }
        }
    }
}
