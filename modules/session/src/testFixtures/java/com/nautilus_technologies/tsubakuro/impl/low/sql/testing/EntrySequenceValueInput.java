package com.nautilus_technologies.tsubakuro.impl.low.sql.testing;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

// import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.low.sql.io.BitBuilder;
import com.nautilus_technologies.tsubakuro.low.sql.io.ByteBuilder;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.nautilus_technologies.tsubakuro.low.sql.io.EntryType;
import com.nautilus_technologies.tsubakuro.low.sql.io.ValueInput;

/**
 * An implementation of {@link ValueInput} which reads from a sequence of {@link Entry}.
 */
public class EntrySequenceValueInput implements ValueInput {

    private static final EnumSet<EntryType> DECIMAL_EXPECT = EnumSet.of(EntryType.DECIMAL, EntryType.INT);

    private final Supplier<? extends Entry> source;

    private Entry current;

    /**
     * Creates a new instance.
     * @param source the input source
     */
    public EntrySequenceValueInput(Supplier<? extends Entry> source) {
        Objects.requireNonNull(source);
        this.source = source;
    }

    /**
     * Creates a new instance.
     * @param collection the entries
     */
    public EntrySequenceValueInput(Iterable<? extends Entry> collection) {
        this(Objects.requireNonNull(collection).iterator());
    }

    /**
     * Creates a new instance.
     * @param iterator the entry iterator
     */
    public EntrySequenceValueInput(Iterator<? extends Entry> iterator) {
        Objects.requireNonNull(iterator);
        this.source = () -> iterator.hasNext() ? iterator.next() : Entry.END_OF_CONTENTS;
    }

    @Override
    public EntryType peekType() {
        if (current == null) {
            current = source.get();
        }
        return current.getType();
    }

    @Override
    public boolean skip(boolean deep) {
        var type = peekType();
        switch (type) {
        case ROW: {
            int size = current.getRowSize();
            discard();
            if (deep) {
                return skipN(size);
            }
            return true;
        }

        case ARRAY: {
            int size = current.getArraySize();
            discard();
            if (deep) {
                return skipN(size);
            }
            return true;
        }

        case END_OF_CONTENTS:
            return false;

        default:
            discard();
            return true;
        }
    }

    private void discard() {
        if (current == null) {
            current = source.get();
        }
        if (!current.isEndOfContentsValue()) {
            current = null;
        }
    }

    private boolean skipN(int count) {
        for (int i = 0; i < count; i++) {
            if (!skip(true)) {
                return false;
            }
        }
        return true;
    }

    private Entry fetchNext(EntryType expected) {
        assert expected != null;
        EntryType found = peekType();
        if (found != expected) {
            throw new IllegalStateException(MessageFormat.format(
                    "inconsistent value type: ''{0}'' is found, but ''{1}'' was expected",
                    found,
                    expected));
        }
        var next = current;
        discard();
        return next;
    }

    private Entry fetchNext(Set<EntryType> expected) {
        assert expected != null;
        EntryType found = peekType();
        if (!expected.contains(found)) {
            throw new IllegalStateException(MessageFormat.format(
                    "inconsistent value type: ''{0}'' is found, but one of ''{1}'' was expected",
                    found,
                    expected));
        }
        var next = current;
        discard();
        return next;
    }

    @Override
    public void readNull() {
        fetchNext(EntryType.NULL);
    }

    @Override
    public long readInt() {
        var next = fetchNext(EntryType.INT);
        return next.getIntValue();
    }

    @Override
    public float readFloat4() {
        var next = fetchNext(EntryType.FLOAT4);
        return next.getFloat4Value();
    }

    @Override
    public double readFloat8() {
        var next = fetchNext(EntryType.FLOAT8);
        return next.getFloat8Value();
    }

    @Override
    public BigDecimal readDecimal() {
        var next = fetchNext(DECIMAL_EXPECT);
        if (next.getType() == EntryType.INT) {
            return BigDecimal.valueOf(next.getIntValue());
        }
        assert next.getType() == EntryType.DECIMAL;
        return next.getDecimalValue();
    }

    @Override
    public String readCharacter() {
        var next = fetchNext(EntryType.CHARACTER);
        return next.getCharacterValue();
    }

    @Override
    public StringBuilder readCharacter(StringBuilder buffer) {
        Objects.requireNonNull(buffer);
        buffer.setLength(0);
        buffer.append(readCharacter());
        return buffer;
    }

    @Override
    public byte[] readOctet() {
        var next = fetchNext(EntryType.OCTET);
        return next.getOctetValue();
    }

    @Override
    public ByteBuilder readOctet(ByteBuilder buffer) {
        Objects.requireNonNull(buffer);
        byte[] results = readOctet();
        buffer.setSize(results.length);
        System.arraycopy(results, 0, buffer.getData(), 0, results.length);
        return buffer;
    }

    @Override
    public boolean[] readBit() {
        var next = fetchNext(EntryType.BIT);
        return next.getBitValue();
    }

    @Override
    public BitBuilder readBit(BitBuilder buffer) {
        Objects.requireNonNull(buffer);
        boolean[] results = readBit();
        buffer.setSize(results.length);
        for (int i = 0; i < results.length; i++) {
            buffer.set(i, results[i]);
        }
        return buffer;
    }

    @Override
    public LocalDate readDate() {
        var next = fetchNext(EntryType.DATE);
        return next.getDateValue();
    }

    @Override
    public LocalTime readTimeOfDay() {
        var next = fetchNext(EntryType.TIME_OF_DAY);
        return next.getTimeOfDayValue();
    }

    @Override
    public Instant readTimePoint() {
        var next = fetchNext(EntryType.TIME_POINT);
        return next.getTimePointValue();
    }

    @Override
    public DateTimeInterval readDateTimeInterval() {
        var next = fetchNext(EntryType.DATETIME_INTERVAL);
        return next.getDateTimeIntervalValue();
    }

    @Override
    public int readRowBegin() {
        var next = fetchNext(EntryType.ROW);
        return next.getRowSize();
    }

    @Override
    public int readArrayBegin() {
        var next = fetchNext(EntryType.ARRAY);
        return next.getArraySize();
    }

    @Override
    public void readEndOfContents() {
        fetchNext(EntryType.END_OF_CONTENTS);
        // force consume
        current = null;
    }

    @Override
    public void close() {
        // do nothing
        return;
    }
}
