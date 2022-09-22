package com.tsurugidb.tsubakuro.sql.impl.testing;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;

// import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.sql.io.BitBuilder;
import com.tsurugidb.tsubakuro.sql.io.ByteBuilder;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;
import com.tsurugidb.tsubakuro.sql.io.ValueOutput;

/**
 * An implementation of {@link ValueOutput} which writes into a sequence of {@link Entry}.
 */
public class EntrySequenceValueOutput implements ValueOutput {

    private final Consumer<? super Entry> destination;

    /**
     * Creates a new instance.
     * @param destination the output destination
     */
    public EntrySequenceValueOutput(Consumer<? super Entry> destination) {
        Objects.requireNonNull(destination);
        this.destination = destination;
    }

    /**
     * Creates a new instance.
     * @param destination the output destination
     */
    public EntrySequenceValueOutput(Collection<? super Entry> destination) {
        Objects.requireNonNull(destination);
        this.destination = destination::add;
    }

    @Override
    public void writeNull() {
        destination.accept(Entry.forNull());
    }

    @Override
    public void writeInt(long value) {
        destination.accept(Entry.forInt(value));
    }

    @Override
    public void writeFloat4(float value) {
        destination.accept(Entry.forFloat4(value));
    }

    @Override
    public void writeFloat8(double value) {
        destination.accept(Entry.forFloat8(value));
    }

    @Override
    public void writeDecimal(BigDecimal value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forDecimal(value));
    }

    @Override
    public void writeCharacter(String value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forCharacter(value));
    }

    @Override
    public void writeCharacter(StringBuilder buffer) {
        Objects.requireNonNull(buffer);
        writeCharacter(buffer.toString());
    }

    @Override
    public void writeOctet(byte[] value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forOctet(value.clone()));
    }

    @Override
    public void writeOctet(byte[] value, int offset, int length) {
        Objects.requireNonNull(value);
        Objects.checkFromIndexSize(offset, length, value.length);
        writeOctet(Arrays.copyOfRange(value, offset, offset + length));
    }

    @Override
    public void writeOctet(ByteBuilder buffer) {
        Objects.requireNonNull(buffer);
        writeOctet(buffer.build());
    }

    @Override
    public void writeBit(boolean[] value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forBit(value.clone()));
    }

    @Override
    public void writeBit(boolean[] value, int offset, int length) {
        Objects.requireNonNull(value);
        Objects.checkFromIndexSize(offset, length, value.length);
        destination.accept(Entry.forBit(Arrays.copyOfRange(value, offset, offset + length)));
    }

    @Override
    public void writeBit(BitBuilder buffer) {
        Objects.requireNonNull(buffer);
        writeBit(buffer.build());
    }

    @Override
    public void writeDate(LocalDate value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forDate(value));
    }

    @Override
    public void writeTimeOfDay(LocalTime value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forTimeOfDay(value));
    }

    @Override
    public void writeTimePoint(LocalDateTime value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forTimePoint(value));
    }

    @Override
    public void writeTimeOfDayWithTimeZone(OffsetTime value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forTimeOfDayWithTimeZone(value));
    }

    @Override
    public void writeTimePointWithTimeZone(OffsetDateTime value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forTimePointWithTimeZone(value));
    }

    @Override
    public void writeDateTimeInterval(DateTimeInterval value) {
        Objects.requireNonNull(value);
        destination.accept(Entry.forDateTimeInterval(value));
    }

    @Override
    public void writeRowBegin(int numberOfElements) {
        destination.accept(Entry.forRow(numberOfElements));
    }

    @Override
    public void writeArrayBegin(int numberOfElements) {
        destination.accept(Entry.forArray(numberOfElements));
    }

    @Override
    public void writeEndOfContents() {
        destination.accept(Entry.forEndOfContents());
    }

    @Override
    public void flush() {
        return;
    }

    @Override
    public void close() {
        return;
    }
}
