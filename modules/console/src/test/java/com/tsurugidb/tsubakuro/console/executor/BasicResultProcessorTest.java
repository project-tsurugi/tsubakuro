package com.tsurugidb.tsubakuro.console.executor;

import static com.nautilus_technologies.tsubakuro.low.sql.Types.column;
import static com.nautilus_technologies.tsubakuro.low.sql.Types.row;
import static com.nautilus_technologies.tsubakuro.low.sql.Types.user;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.nautilus_technologies.tsubakuro.impl.low.sql.ResultSetMetadataAdapter;
import com.nautilus_technologies.tsubakuro.impl.low.sql.testing.Relation;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetMetadata;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlResponse;

class BasicResultProcessorTest {

    static final Logger LOG = LoggerFactory.getLogger(BasicResultProcessorTest.class);

    private final IoSupplier<Writer> sink = new IoSupplier<>() {

        @Override
        public Writer get() throws IOException {
            return new StringWriter() {
                private boolean closed = false;
                @Override
                public void close() throws IOException {
                    if (closed) {
                        return;
                    }
                    outputs.add(toString());
                    closed = true;
                }
            };
        }
    };

    private final List<String> outputs = new ArrayList<>();

    @AfterEach
    void dump() {
        for (String s : outputs) {
            for (String line : s.split(Pattern.quote(System.lineSeparator()))) {
                LOG.debug(line);
            }
        }
    }

    @Test
    void simple() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { 1 },
        }).getResultSet(meta(column("a", int.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void matrix() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { 1, "A", new BigDecimal("100") },
            { 2, "B", new BigDecimal("200") },
            { 3, "C", new BigDecimal("300") },
        }).getResultSet(meta(column(int.class), column(String.class), column(BigDecimal.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void header_array() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {})
                .getResultSet(meta(column("a", int[].class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void header_row() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {})
                .getResultSet(meta(column("a", row(column(int.class), column(String.class), column(BigDecimal.class)))));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void header_user() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {})
                .getResultSet(meta(column("a", user("U"))));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_null() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { null },
        }).getResultSet(meta(column(int.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_boolean() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { true },
        }).getResultSet(meta(column(boolean.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_int4() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { 125 },
        }).getResultSet(meta(column(long.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_int8() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { 1L },
        }).getResultSet(meta(column(long.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_float4() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { 1.f },
        }).getResultSet(meta(column(float.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_float8() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { 1.d },
        }).getResultSet(meta(column(double.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_decimal() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { new BigDecimal("3.14") },
        }).getResultSet(meta(column(BigDecimal.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_character() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { "Hello, world!" },
        }).getResultSet(meta(column(String.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_octet() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { new byte[] { 1, 2, 3 } },
        }).getResultSet(meta(column(byte[].class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_bit() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { new boolean[] { true, false } },
        }).getResultSet(meta(column(boolean[].class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_date() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { LocalDate.of(2000, 1, 1) },
        }).getResultSet(meta(column(LocalDate.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_time_point() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { Instant.ofEpochSecond(123) },
        }).getResultSet(meta(column(Instant.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_time_of_day() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { LocalTime.of(1, 2, 3) },
        }).getResultSet(meta(column(LocalTime.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_date_time_interval() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { new DateTimeInterval(1, 2, 3, 4L) },
        }).getResultSet(meta(column(DateTimeInterval.class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_array() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            {  Relation.array(1, 2, 3) },
        }).getResultSet(meta(column(int[].class)));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    @Test
    void value_row() throws Exception {
        ResultSet rs = Relation.of(new Object[][] {
            { Relation.row(1, "OK", new BigDecimal("3.14")) },
        }).getResultSet(meta(column(row(column(int.class), column(String.class), column(BigDecimal.class)))));

        try (var proc = create()) {
            proc.process(rs);
        }
        assertEquals(1, outputs.size());
    }

    private BasicResultProcessor create() {
        return new BasicResultProcessor(sink, new JsonFactory());
    }

    private static ResultSetMetadata meta(SqlCommon.Column... columns) {
        return new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.newBuilder()
                .addAllColumns(Arrays.asList(columns))
                .build());
    }
}
