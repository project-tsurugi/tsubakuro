package com.tsurugidb.tsubakuro.sql.io;

import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_ARRAY;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_BIT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_CHARACTER;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_DATE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_DATETIME_INTERVAL;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_DECIMAL;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_DECIMAL_COMPACT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_ARRAY;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_BIT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_CHARACTER;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_NEGATIVE_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_OCTET;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_POSITIVE_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_ROW;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_END_OF_CONTENTS;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_FLOAT4;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_FLOAT8;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_OCTET;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_ROW;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_TIME_OF_DAY;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_TIME_POINT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_TIME_OF_DAY_WITH_TIME_ZONE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_TIME_POINT_WITH_TIME_ZONE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_DECIMAL_COMPACT_COEFFICIENT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_EMBED_ARRAY_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_EMBED_BIT_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_EMBED_CHARACTER_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_EMBED_NEGATIVE_INT_VALUE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_EMBED_OCTET_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_EMBED_POSITIVE_INT_VALUE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MAX_EMBED_ROW_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_DECIMAL_COMPACT_COEFFICIENT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_ARRAY_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_BIT_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_CHARACTER_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_NEGATIVE_INT_VALUE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_OCTET_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_POSITIVE_INT_VALUE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_ROW_SIZE;

import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ValueOutput} to {@link OutputStream}.
 * @see StreamBackedValueInput
 */
@NotThreadSafe
public class StreamBackedValueOutput implements ValueOutput, Flushable {

    static final Logger LOG = LoggerFactory.getLogger(StreamBackedValueOutput.class);

    private final OutputStream output;

    /**
     * Creates a new instance.
     * @param output the target output
     */
    public StreamBackedValueOutput(@Nonnull OutputStream output) {
        Objects.requireNonNull(output);
        this.output = output;
    }

    @Override
    public void writeNull() throws IOException {
        output.write(Constants.HEADER_UNKNOWN);
    }

    @Override
    public void writeInt(long value) throws IOException {
        if (MIN_EMBED_POSITIVE_INT_VALUE <= value && value <= MAX_EMBED_POSITIVE_INT_VALUE) {
            output.write(HEADER_EMBED_POSITIVE_INT | ((int) value - MIN_EMBED_POSITIVE_INT_VALUE));
        } else if (MIN_EMBED_NEGATIVE_INT_VALUE <= value && value <= MAX_EMBED_NEGATIVE_INT_VALUE) {
            output.write(HEADER_EMBED_NEGATIVE_INT | ((int) value - MIN_EMBED_NEGATIVE_INT_VALUE));
        } else {
            output.write(HEADER_INT);
            Base128Variant.writeSigned(value, output);
        }
    }

    @Override
    public void writeFloat4(float value) throws IOException {
        output.write(HEADER_FLOAT4);
        write4(Float.floatToIntBits(value));
    }

    @Override
    public void writeFloat8(double value) throws IOException {
        output.write(HEADER_FLOAT8);
        write8(Double.doubleToLongBits(value));
    }

    @Override
    public void writeDecimal(@Nonnull BigDecimal value) throws IOException {
        Objects.requireNonNull(value);
        if (writeAsLong(value)) {
            return;
        }

        BigInteger coefficient = value.unscaledValue();
        if (MIN_DECIMAL_COMPACT_COEFFICIENT.compareTo(coefficient) <= 0
                && coefficient.compareTo(MAX_DECIMAL_COMPACT_COEFFICIENT) <= 0) {
            output.write(HEADER_DECIMAL_COMPACT);
            Base128Variant.writeSigned(-value.scale(), output);
            Base128Variant.writeSigned(coefficient.longValue(), output);
            return;
        }

        output.write(HEADER_DECIMAL);
        Base128Variant.writeSigned(-value.scale(), output);

        byte[] coefficientBytes = coefficient.toByteArray();
        Base128Variant.writeUnsigned(coefficientBytes.length, output);
        output.write(coefficientBytes);
    }

    private static final BigDecimal MAX_LONG_VALUE_AS_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG_VALUE_AS_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

    private boolean writeAsLong(BigDecimal value) throws IOException {
        // only considers scale = 0
        if (value.scale() != 0
                || value.compareTo(MIN_LONG_VALUE_AS_DECIMAL) < 0
                || value.compareTo(MAX_LONG_VALUE_AS_DECIMAL) > 0) {
            return false;
        }
        long integer = value.longValueExact();
        writeInt(integer);
        return true;
    }

    @Override
    public void writeCharacter(@Nonnull String value) throws IOException {
        Objects.requireNonNull(value);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeCharacterHead(bytes.length);
        output.write(bytes);
    }

    @Override
    public void writeCharacter(StringBuilder buffer) throws IOException {
        Objects.requireNonNull(buffer);
        writeCharacter(buffer.toString());
    }

    private void writeCharacterHead(int length) throws IOException {
        if (MIN_EMBED_CHARACTER_SIZE <= length && length <= MAX_EMBED_CHARACTER_SIZE) {
            output.write(HEADER_EMBED_CHARACTER | (length - MIN_EMBED_CHARACTER_SIZE));
        } else {
            output.write(HEADER_CHARACTER);
            Base128Variant.writeUnsigned(length, output);
        }
    }

    @Override
    public void writeOctet(byte[] value) throws IOException {
        Objects.requireNonNull(value);
        writeOctet(value, 0, value.length);
    }

    @Override
    public void writeOctet(@Nonnull byte[] value, int offset, int length) throws IOException {
        Objects.requireNonNull(value);
        Objects.checkFromIndexSize(offset, length, value.length);
        writeOctetHead(length);
        output.write(value, offset, length);
    }

    @Override
    public void writeOctet(ByteBuilder buffer) throws IOException {
        Objects.requireNonNull(buffer);
        writeOctet(buffer.getData(), 0, buffer.getSize());
    }

    private void writeOctetHead(int length) throws IOException {
        if (MIN_EMBED_OCTET_SIZE <= length && length <= MAX_EMBED_OCTET_SIZE) {
            output.write(HEADER_EMBED_OCTET | (length - MIN_EMBED_OCTET_SIZE));
        } else {
            output.write(HEADER_OCTET);
            Base128Variant.writeUnsigned(length, output);
        }
    }


    @Override
    public void writeBit(boolean[] value) throws IOException {
        Objects.requireNonNull(value);
        writeBit(value, 0, value.length);
    }

    @Override
    public void writeBit(@Nonnull boolean[] value, int offset, int length) throws IOException {
        Objects.requireNonNull(value);
        Objects.checkFromIndexSize(offset, length, value.length);
        writeBitHead(length);
        for (int i = 0; i < length; i += 8) {
            byte b = packBits(value, i);
            output.write(b);
        }
    }

    @Override
    public void writeBit(@Nonnull BitBuilder buffer) throws IOException {
        Objects.requireNonNull(buffer);
        writeBitHead(buffer.getSize());
        output.write(buffer.getData(), 0, buffer.getByteSize());
    }

    private void writeBitHead(int length) throws IOException {
        if (MIN_EMBED_BIT_SIZE <= length && length <= MAX_EMBED_BIT_SIZE) {
            output.write(HEADER_EMBED_BIT | (length - MIN_EMBED_BIT_SIZE));
        } else {
            output.write(HEADER_BIT);
            Base128Variant.writeUnsigned(length, output);
        }
    }

    private static byte packBits(boolean[] value, int offset) {
        assert value != null;
        assert offset < value.length;
        byte result = 0;
        for (int i = 0; i < 8 && i + offset < value.length; i++) {
            if (value[i + offset]) {
                result |= 1 << i;
            }
        }
        return result;
    }

    @Override
    public void writeDate(@Nonnull LocalDate value) throws IOException {
        Objects.requireNonNull(value);
        output.write(HEADER_DATE);

        var offset = value.toEpochDay();
        Base128Variant.writeSigned(offset, output);
    }

    @Override
    public void writeTimeOfDay(@Nonnull LocalTime value) throws IOException {
        Objects.requireNonNull(value);
        output.write(HEADER_TIME_OF_DAY);

        var offset = value.toNanoOfDay();
        assert offset >= 0;
        Base128Variant.writeUnsigned(offset, output);
    }

    @Override
    public void writeTimePoint(@Nonnull LocalDateTime value) throws IOException {
        Objects.requireNonNull(value);
        output.write(HEADER_TIME_POINT);
        var offsetSecond = 24 * 3600 * value.toLocalDate().toEpochDay() + value.toLocalTime().toSecondOfDay();
        var offsetNano = value.getNano();
        Base128Variant.writeSigned(offsetSecond, output);
        Base128Variant.writeUnsigned(offsetNano, output);
    }

    @Override
    public void writeTimeOfDayWithTimeZone(@Nonnull OffsetTime value) throws IOException {
        Objects.requireNonNull(value);
        output.write(HEADER_TIME_OF_DAY_WITH_TIME_ZONE);
        var offset = value.toLocalTime().toNanoOfDay();
        assert offset >= 0;
        var timeZoneOffset = value.getOffset().getTotalSeconds() / 60;
        Base128Variant.writeUnsigned(offset, output);
        Base128Variant.writeSigned(timeZoneOffset, output);
    }

    @Override
    public void writeTimePointWithTimeZone(@Nonnull OffsetDateTime value) throws IOException {
        Objects.requireNonNull(value);
        output.write(HEADER_TIME_POINT_WITH_TIME_ZONE);
        var localDateTime = value.toLocalDateTime();
        var offsetSecond = 24 * 3600 * localDateTime.toLocalDate().toEpochDay() + localDateTime.toLocalTime().toSecondOfDay();
        var offsetNano = value.getNano();
        var timeZoneOffset = value.getOffset().getTotalSeconds() / 60;
        Base128Variant.writeSigned(offsetSecond, output);
        Base128Variant.writeUnsigned(offsetNano, output);
        Base128Variant.writeSigned(timeZoneOffset, output);
    }

    @Override
    public void writeDateTimeInterval(@Nonnull DateTimeInterval value) throws IOException {
        Objects.requireNonNull(value);
        output.write(HEADER_DATETIME_INTERVAL);

        var year = value.getYear();
        var month = value.getMonth();
        var day = value.getDay();
        var nanos = value.getNanoseconds();
        Base128Variant.writeSigned(year, output);
        Base128Variant.writeSigned(month, output);
        Base128Variant.writeSigned(day, output);
        Base128Variant.writeSigned(nanos, output);
    }

    @Override
    public void writeRowBegin(int numberOfElements) throws IOException {
        checkPositive(numberOfElements);
        if (MIN_EMBED_ROW_SIZE <= numberOfElements && numberOfElements <= MAX_EMBED_ROW_SIZE) {
            output.write(HEADER_EMBED_ROW | (numberOfElements - MIN_EMBED_ROW_SIZE));
        } else {
            output.write(HEADER_ROW);
            Base128Variant.writeUnsigned(numberOfElements, output);
        }
    }

    @Override
    public void writeArrayBegin(int numberOfElements) throws IOException {
        checkPositive(numberOfElements);
        if (MIN_EMBED_ARRAY_SIZE <= numberOfElements && numberOfElements <= MAX_EMBED_ARRAY_SIZE) {
            output.write(HEADER_EMBED_ARRAY | (numberOfElements - MIN_EMBED_ARRAY_SIZE));
        } else {
            output.write(HEADER_ARRAY);
            Base128Variant.writeUnsigned(numberOfElements, output);
        }
    }

    @Override
    public void writeEndOfContents() throws IOException {
        output.write(HEADER_END_OF_CONTENTS);
    }

    /**
     * Invokes {@link OutputStream#flush()} of the backed {@link OutputStream}.
     */
    @Override
    public void flush() throws IOException {
        output.flush();
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

    private static void checkPositive(int numberOfElements) {
        if (numberOfElements < 0) {
            throw new IllegalArgumentException();
        }
    }

    private void write4(int value) throws IOException {
        output.write(value >>> 24);
        output.write(value >>> 16);
        output.write(value >>> 8);
        output.write(value >>> 0);
    }

    private void write8(long value) throws IOException {
        write4((int) (value >>> 32));
        write4((int) value);
    }
}
