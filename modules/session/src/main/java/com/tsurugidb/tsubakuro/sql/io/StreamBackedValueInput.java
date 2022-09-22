package com.tsurugidb.tsubakuro.sql.io;

import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_ARRAY;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_BIT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_CHARACTER;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_DECIMAL;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_DECIMAL_COMPACT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_ARRAY;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_BIT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_CHARACTER;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_NEGATIVE_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_OCTET;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_POSITIVE_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_EMBED_ROW;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_OCTET;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_ROW;
import static com.tsurugidb.tsubakuro.sql.io.Constants.HEADER_UNKNOWN;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MASK_EMBED_ARRAY;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MASK_EMBED_BIT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MASK_EMBED_CHARACTER;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MASK_EMBED_NEGATIVE_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MASK_EMBED_OCTET;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MASK_EMBED_POSITIVE_INT;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MASK_EMBED_ROW;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_ARRAY_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_BIT_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_CHARACTER_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_NEGATIVE_INT_VALUE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_OCTET_SIZE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_POSITIVE_INT_VALUE;
import static com.tsurugidb.tsubakuro.sql.io.Constants.MIN_EMBED_ROW_SIZE;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ValueInput} from {@link InputStream}.
 * @see StreamBackedValueOutput
 */
@NotThreadSafe
public class StreamBackedValueInput implements ValueInput {

    static final Logger LOG = LoggerFactory.getLogger(StreamBackedValueInput.class);

    private static final int HEADER_HARD_EOF = -1;

    private static final int HEADER_UNGAINED = -2;

    private static final int OFFSET_INDEPENDENT_ENTRY_TYPE = -HEADER_UNKNOWN;

    private static final EntryType[] INDEPENDENT_ENTRY_TYPE = {
            // 0xe8
            EntryType.NULL,
            // 0xe9
            EntryType.INT,
            // 0xea
            EntryType.FLOAT4,
            // 0xeb
            EntryType.FLOAT8,
            // 0xec
            EntryType.DECIMAL,
            // 0xed
            EntryType.DECIMAL,
            // 0xee
            EntryType.TIME_OF_DAY_WITH_TIME_ZONE,
            // 0xef
            EntryType.TIME_POINT_WITH_TIME_ZONE,
            // 0xf0
            EntryType.CHARACTER,
            // 0xf1
            EntryType.OCTET,
            // 0xf2
            EntryType.BIT,
            // 0xf3
            EntryType.DATE,
            // 0xf4
            EntryType.TIME_OF_DAY,
            // 0xf5
            EntryType.TIME_POINT,
            // 0xf6
            EntryType.DATETIME_INTERVAL,
            // 0xf7
            null,
            // 0xf8
            EntryType.ROW,
            // 0xf9
            EntryType.ARRAY,
            // 0xfa
            null, // EntryType.CLOB,
            // 0xfb
            null, // EntryType.BLOB,
            // 0xfc
            null,
            // 0xfd
            null,
            // 0xfe
            EntryType.END_OF_CONTENTS,
            // 0xff
            null,
    };

    private final InputStream input;

    private boolean sawEof = false;

    private EntryType currentEntryType;

    private int currentHeaderCategory;

    private int currentHeaderPayload;

    private final ByteBuilder byteBuilder = new ByteBuilder();

    private final BitBuilder bitBuilder = new BitBuilder();

    private final byte[] readBuffer = new byte[16]; // don't change buffer size, it's for decimal128 input

    /**
     * Creates a new instance.
     * @param input the source input
     */
    public StreamBackedValueInput(@Nonnull InputStream input) {
        Objects.requireNonNull(input);
        this.input = input;
    }

    @Override
    public EntryType peekType() throws IOException {
        if (currentEntryType == null) {
            fetchHeader();
            if (LOG.isTraceEnabled()) {
                LOG.trace("read entry: {} ({})", currentEntryType, currentHeaderCategory); //$NON-NLS-1$
            }
        }
        assert currentEntryType != null;
        return currentEntryType;
    }

    private void clearHeaderInfo() {
        currentEntryType = null;
        currentHeaderCategory = HEADER_UNGAINED;
        currentHeaderPayload = 0;
    }

    private void fetchHeader() throws IOException {
        if (sawEof) {
            currentEntryType = EntryType.END_OF_CONTENTS;
            currentHeaderCategory = HEADER_HARD_EOF;
            currentHeaderPayload = 0;
            return;
        }
        int c = input.read();
        assert c <= 0xff;
        if (c < 0) {
            sawEof = true;
            currentEntryType = EntryType.END_OF_CONTENTS;
        } else if (c <= (HEADER_EMBED_POSITIVE_INT | MASK_EMBED_POSITIVE_INT)) {
            currentEntryType = EntryType.INT;
            currentHeaderCategory = HEADER_EMBED_POSITIVE_INT;
            currentHeaderPayload = c & MASK_EMBED_POSITIVE_INT;
        } else if (c <= (HEADER_EMBED_CHARACTER | MASK_EMBED_CHARACTER)) {
            currentEntryType = EntryType.CHARACTER;
            currentHeaderCategory = HEADER_EMBED_CHARACTER;
            currentHeaderPayload = c & MASK_EMBED_CHARACTER;
        } else if (c <= (HEADER_EMBED_ROW | MASK_EMBED_ROW)) {
            currentEntryType = EntryType.ROW;
            currentHeaderCategory = HEADER_EMBED_ROW;
            currentHeaderPayload = c & MASK_EMBED_ROW;
        } else if (c <= (HEADER_EMBED_ARRAY | MASK_EMBED_ARRAY)) {
            currentEntryType = EntryType.ARRAY;
            currentHeaderCategory = HEADER_EMBED_ARRAY;
            currentHeaderPayload = c & MASK_EMBED_ARRAY;
        } else if (c <= (HEADER_EMBED_NEGATIVE_INT | MASK_EMBED_NEGATIVE_INT)) {
            currentEntryType = EntryType.INT;
            currentHeaderCategory = HEADER_EMBED_NEGATIVE_INT;
            currentHeaderPayload = c & MASK_EMBED_NEGATIVE_INT;
        } else if (c <= (HEADER_EMBED_OCTET | MASK_EMBED_OCTET)) {
            currentEntryType = EntryType.OCTET;
            currentHeaderCategory = HEADER_EMBED_OCTET;
            currentHeaderPayload = c & MASK_EMBED_OCTET;
        } else if (c <= (HEADER_EMBED_BIT | MASK_EMBED_BIT)) {
            currentEntryType = EntryType.BIT;
            currentHeaderCategory = HEADER_EMBED_BIT;
            currentHeaderPayload = c & MASK_EMBED_BIT;
        } else {
            int index = c + OFFSET_INDEPENDENT_ENTRY_TYPE;
            assert index >= 0;
            assert index < INDEPENDENT_ENTRY_TYPE.length;
            var type = INDEPENDENT_ENTRY_TYPE[index];
            if (type == null) {
                throw BrokenEncodingException.sawUnrecognizedEntry(c);
            }
            currentEntryType = type;
            currentHeaderCategory = c;
            currentHeaderPayload = 0;
        }
    }

    @Override
    public boolean skip(boolean deep) throws IOException {
        EntryType type = peekType();
        switch (type) {
        case NULL:
            readNull();
            return true;

        case INT:
            readInt();
            return true;
        case FLOAT4:
            readFloat4();
            return true;
        case FLOAT8:
            readFloat8();
            return true;
        case DECIMAL:
            readDecimal();
            return true;

        case CHARACTER:
            readCharacter();
            return true;
        case BIT:
            readBit(bitBuilder);
            return true;
        case OCTET:
            readOctet(byteBuilder);
            return true;

        case DATE:
            readDate();
            return true;
        case TIME_OF_DAY:
            readTimeOfDay();
            return true;
        case TIME_POINT:
            readTimePoint();
            return true;
        case TIME_OF_DAY_WITH_TIME_ZONE:
            readTimeOfDayWithTimeZone();
            return true;
        case TIME_POINT_WITH_TIME_ZONE:
            readTimePointWithTimeZone();
            return true;
        case DATETIME_INTERVAL:
            readDateTimeInterval();
            return true;

        case ROW: {
            int count = readRowBegin();
            if (deep) {
                return skipN(count);
            }
            return true;
        }
        case ARRAY: {
            int count = readArrayBegin();
            if (deep) {
                return skipN(count);
            }
            return true;
        }
        case END_OF_CONTENTS:
            // keep entry
            return false;
        default:
            throw BrokenEncodingException.sawUnsupportedEntry(type);
        }
    }

    private boolean skipN(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            if (!skip(true)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void readNull() throws IOException {
        require(EntryType.NULL);
        clearHeaderInfo();
        // header only
    }

    @Override
    public long readInt() throws IOException {
        require(EntryType.INT);
        return readIntBody();
    }

    private long readIntBody() throws IOException {
        int category = currentHeaderCategory;
        int payload = currentHeaderPayload;
        clearHeaderInfo();
        if (category == HEADER_EMBED_POSITIVE_INT) {
            return payload + MIN_EMBED_POSITIVE_INT_VALUE;
        } else if (category == HEADER_EMBED_NEGATIVE_INT) {
            return payload + MIN_EMBED_NEGATIVE_INT_VALUE;
        } else {
            assert category == HEADER_INT;
            return Base128Variant.readSigned(input);
        }
    }

    @Override
    public float readFloat4() throws IOException {
        require(EntryType.FLOAT4);
        clearHeaderInfo();
        var result = Float.intBitsToFloat(read4());
        return result;
    }

    @Override
    public double readFloat8() throws IOException {
        require(EntryType.FLOAT8);
        clearHeaderInfo();
        var result = Double.longBitsToDouble(read8());
        return result;
    }

    @Override
    public BigDecimal readDecimal() throws IOException {
        var type = require(EntryType.DECIMAL, EntryType.INT);
        if (type == EntryType.INT) {
            assert type == EntryType.INT;
            long value = readIntBody();
            return BigDecimal.valueOf(value);
        }

        int category = currentHeaderCategory;
        clearHeaderInfo();

        if (category == HEADER_DECIMAL_COMPACT) {
            int scale = readSignedInt32();
            long coefficient = Base128Variant.readSigned(input);
            return BigDecimal.valueOf(coefficient, -scale);
        }

        assert category == HEADER_DECIMAL;
        int exponent = readSignedInt32();

        int coefficientSize = readSize();
        var buf = byteBuilder;
        buf.setSize(coefficientSize, false);
        readN(buf.getData(), 0, buf.getSize());
        var coefficient = new BigInteger(buf.getData(), 0, buf.getSize());

        return new BigDecimal(coefficient, -exponent);
    }

    @Override
    public String readCharacter() throws IOException {
        require(EntryType.CHARACTER);
        var size = readCharacterSize();

        var buf = byteBuilder;
        buf.setSize(size, false);
        readN(buf.getData(), 0, buf.getSize());

        var s = new String(buf.getData(), 0, buf.getSize(), StandardCharsets.UTF_8);
        return s;
    }

    @Override
    public StringBuilder readCharacter(@Nonnull StringBuilder buffer) throws IOException {
        Objects.requireNonNull(buffer);
        buffer.setLength(0);
        buffer.append(readCharacter());
        return buffer;
    }

    private int readCharacterSize() throws IOException {
        int category = currentHeaderCategory;
        int payload = currentHeaderPayload;
        clearHeaderInfo();
        if (category == HEADER_EMBED_CHARACTER) {
            return payload + MIN_EMBED_CHARACTER_SIZE;
        }
        assert category == HEADER_CHARACTER;
        return readSize();
    }

    @Override
    public byte[] readOctet() throws IOException {
        return readOctet(byteBuilder).build();
    }

    @Override
    public ByteBuilder readOctet(@Nonnull ByteBuilder buffer) throws IOException {
        Objects.requireNonNull(buffer);

        require(EntryType.OCTET);
        var size = readOctetSize();

        buffer.setSize(size, false);
        readN(buffer.getData(), 0, buffer.getSize());
        return buffer;
    }

    private int readOctetSize() throws IOException {
        int category = currentHeaderCategory;
        int payload = currentHeaderPayload;
        clearHeaderInfo();
        if (category == HEADER_EMBED_OCTET) {
            return payload + MIN_EMBED_OCTET_SIZE;
        }
        assert category == HEADER_OCTET;
        return readSize();
    }

    @Override
    public boolean[] readBit() throws IOException {
        return readBit(bitBuilder).build();
    }

    @Override
    public BitBuilder readBit(@Nonnull BitBuilder buffer) throws IOException {
        Objects.requireNonNull(buffer);

        require(EntryType.BIT);
        var size = readBitSize();

        buffer.setSize(size, false);
        readN(buffer.getData(), 0, buffer.getByteSize());

        return buffer;
    }

    private int readBitSize() throws IOException {
        int category = currentHeaderCategory;
        int payload = currentHeaderPayload;
        clearHeaderInfo();
        if (category == HEADER_EMBED_BIT) {
            return payload + MIN_EMBED_BIT_SIZE;
        }
        assert category == HEADER_BIT;
        return readSize();
    }

    @Override
    public LocalDate readDate() throws IOException {
        require(EntryType.DATE);
        clearHeaderInfo();
        var offset = Base128Variant.readSigned(input);
        return LocalDate.ofEpochDay(offset);
    }

    @Override
    public LocalTime readTimeOfDay() throws IOException {
        require(EntryType.TIME_OF_DAY);
        clearHeaderInfo();
        var offset = Base128Variant.readUnsigned(input);
        return LocalTime.ofNanoOfDay(offset);
    }

    @Override
    public LocalDateTime readTimePoint() throws IOException {
        require(EntryType.TIME_POINT);
        clearHeaderInfo();
        var seconds = Base128Variant.readSigned(input);
        long nanos = Base128Variant.readUnsigned(input);
        var days = seconds / (24 * 3600);
        return LocalDateTime.of(LocalDate.ofEpochDay(days), LocalTime.ofNanoOfDay(1000_000_000L * (seconds - (24 * 3600 * days)) + nanos));
    }

    @Override
    public OffsetTime readTimeOfDayWithTimeZone() throws IOException {
        require(EntryType.TIME_OF_DAY_WITH_TIME_ZONE);
        clearHeaderInfo();
        var offset = Base128Variant.readUnsigned(input);
        var timeZoneOffsetInMinites = (int) Base128Variant.readSigned(input);
        return OffsetTime.of(LocalTime.ofNanoOfDay(offset), ZoneOffset.ofTotalSeconds(timeZoneOffsetInMinites * 60));
    }

    @Override
    public OffsetDateTime readTimePointWithTimeZone()throws IOException {
        require(EntryType.TIME_POINT_WITH_TIME_ZONE);
        clearHeaderInfo();
        var seconds = Base128Variant.readSigned(input);
        long nanos = Base128Variant.readUnsigned(input);
        var timeZoneOffsetInMinites = (int) Base128Variant.readSigned(input);
        var days = seconds / (24 * 3600);
        return OffsetDateTime.of(LocalDate.ofEpochDay(days), LocalTime.ofNanoOfDay(1000_000_000L * (seconds - (24 * 3600 * days)) + nanos), ZoneOffset.ofTotalSeconds(timeZoneOffsetInMinites * 60));
    }

    @Override
    public DateTimeInterval readDateTimeInterval() throws IOException {
        require(EntryType.DATETIME_INTERVAL);
        clearHeaderInfo();
        var year = readSignedInt32();
        var month = readSignedInt32();
        var day = readSignedInt32();
        var nanos = Base128Variant.readSigned(input);
        return new DateTimeInterval(year, month, day, nanos);
    }

    @Override
    public int readRowBegin() throws IOException {
        require(EntryType.ROW);

        int category = currentHeaderCategory;
        int payload = currentHeaderPayload;
        clearHeaderInfo();

        if (category == HEADER_EMBED_ROW) {
            return payload + MIN_EMBED_ROW_SIZE;
        }
        assert category == HEADER_ROW;
        return readSize();
    }

    @Override
    public int readArrayBegin() throws IOException {
        require(EntryType.ARRAY);

        int category = currentHeaderCategory;
        int payload = currentHeaderPayload;
        clearHeaderInfo();

        if (category == HEADER_EMBED_ARRAY) {
            return payload + MIN_EMBED_ARRAY_SIZE;
        }
        assert category == HEADER_ARRAY;
        return readSize();
    }

    @Override
    public void readEndOfContents() throws IOException {
        require(EntryType.END_OF_CONTENTS);
        clearHeaderInfo();
        // header only
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    private EntryType require(@Nonnull EntryType expected) throws IOException {
        assert expected != null;
        EntryType found = peekType();
        if (expected != found) {
            throw new IllegalStateException(MessageFormat.format(
                    "inconsistent value type: ''{0}'' is found, but''{1}'' was expected",
                    found,
                    expected));
        }
        return found;
    }

    private EntryType require(@Nonnull EntryType expected, @Nonnull EntryType option) throws IOException {
        assert expected != null;
        assert option != null;
        EntryType found = peekType();
        if (expected != found && option != found) {
            throw new IllegalStateException(MessageFormat.format(
                    "inconsistent value type: ''{0}'' is found, but expected ''{1}'' or ''{2}''",
                    found,
                    expected,
                    option));
        }
        return found;
    }

    private int read4() throws IOException {
        var buf = readBuffer;
        readN(buf, 0, Integer.BYTES);
        return (buf[0] & 0xff) << 24
                | (buf[1] & 0xff) << 16
                | (buf[2] & 0xff) << 8
                | (buf[3] & 0xff);
    }

    private long read8() throws IOException {
        var buf = readBuffer;
        readN(buf, 0, Long.BYTES);
        return (buf[0] & 0xffL) << 56
                | (buf[1] & 0xffL) << 48
                | (buf[2] & 0xffL) << 40
                | (buf[3] & 0xffL) << 32
                | (buf[4] & 0xffL) << 24
                | (buf[5] & 0xffL) << 16
                | (buf[6] & 0xffL) << 8
                | (buf[7] & 0xffL);
    }

    private void readN(byte[] buf, int offset, int length) throws IOException {
        int read = input.readNBytes(buf, offset, length);
        if (read != length) {
            throw BrokenEncodingException.sawUnexpectedEof();
        }
    }

    private int readSignedInt32() throws IOException {
        long value = Base128Variant.readSigned(input);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw BrokenEncodingException.sawSignedInt32OutOfRange(value);
        }
        return (int) value;
    }

    private int readSize() throws IOException {
        long value = Base128Variant.readUnsigned(input);
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw BrokenEncodingException.sawUnsupportedSize(value);
        }
        return (int) value;
    }
}
