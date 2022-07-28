package com.nautilus_technologies.tsubakuro.low.sql.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.impl.low.sql.TableMetadataAdapter;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;

class LoadBuilderTest {

    private static final Pattern PATTERN_TOKEN = Pattern.compile("(\\w+|[(),\\.]|\".*\"|:\\w+)|[ \\t\\n\\r]+");

    private final SqlResponse.DescribeTable.Success table = SqlResponse.DescribeTable.Success.newBuilder()
            .setTableName("T")
            .setSchemaName("S")
            .setDatabaseName("D")
            .addColumns(SqlCommon.Column.newBuilder()
                    .setName("C1")
                    .setAtomType(SqlCommon.AtomType.INT4))
            .addColumns(SqlCommon.Column.newBuilder()
                    .setName("C2")
                    .setAtomType(SqlCommon.AtomType.CHARACTER))
            .addColumns(SqlCommon.Column.newBuilder()
                    .setName("C3")
                    .setAtomType(SqlCommon.AtomType.DECIMAL))
            .build();

    private final SqlClient client = new SqlClient() {
        @Override
        public FutureResponse<PreparedStatement> prepare(
                String source,
                Collection<? extends SqlRequest.PlaceHolder> placeholders) throws IOException {
            captureSource = tokenize(source);
            capturePlaceholders = placeholders.stream()
                    .sorted((a, b) -> a.getName().compareTo(b.getName()))
                    .collect(Collectors.toList());
            return FutureResponse.returns(new PreparedStatement() {
                @Override
                public boolean hasResultRecords() {
                    return false;
                }

                @Override
                public void setCloseTimeout(long timeout, TimeUnit unit) {
                    return;
                }

                @Override
                public void close() {
                    return;
                }
            });
        }
    };

    private final Transaction transaction = new Transaction() {
        @Override
        public FutureResponse<Void> executeLoad(
                PreparedStatement statement,
                Collection<? extends SqlRequest.Parameter> parameters,
                Collection<? extends Path> files) throws IOException {
            captureParameters = parameters.stream()
                    .sorted((a, b) -> a.getName().compareTo(b.getName()))
                    .collect(Collectors.toList());
            captureFiles = files.stream()
                    .sorted()
                    .collect(Collectors.toList());
            return FutureResponse.returns(null);
        }
    };

    private List<String> captureSource;

    private List<? extends SqlRequest.PlaceHolder> capturePlaceholders;

    private List<? extends SqlRequest.Parameter> captureParameters;

    private List<? extends Path> captureFiles;

    @Test
    void test() throws Exception {
        var cols = table.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(table))
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        var as = captureParameters;
        assertEquals(1, ps.size());
        assertEquals(ps.size(), as.size());

        assertEquals(SqlCommon.AtomType.INT4, ps.get(0).getAtomType());
        assertEquals("L0", as.get(0).getReferenceColumnName());

        assertEquals(
                tokenize("INSERT INTO D.S.T (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);

        assertEquals(List.of(Path.of("testing")), captureFiles);
    }

    @Test
    void testPosition() throws Exception {
        var cols = table.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(table))
                    .mapping(cols.get(0), 3)
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        var as = captureParameters;
        assertEquals(1, ps.size());
        assertEquals(ps.size(), as.size());

        assertEquals(SqlCommon.AtomType.INT4, ps.get(0).getAtomType());
        assertEquals(3, as.get(0).getReferenceColumnPosition());

        assertEquals(
                tokenize("INSERT INTO D.S.T (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);

        assertEquals(List.of(Path.of("testing")), captureFiles);
    }

    @Test
    void testColumns() throws Exception {
        var cols = table.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(table))
                    .mapping(cols.get(0), "L0")
                    .mapping(cols.get(1), "L1")
                    .mapping(cols.get(2), "L2")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        var as = captureParameters;
        assertEquals(3, ps.size());
        assertEquals(ps.size(), as.size());

        assertEquals(SqlCommon.AtomType.INT4, ps.get(0).getAtomType());
        assertEquals("L0", as.get(0).getReferenceColumnName());

        assertEquals(SqlCommon.AtomType.CHARACTER, ps.get(1).getAtomType());
        assertEquals("L1", as.get(1).getReferenceColumnName());

        assertEquals(SqlCommon.AtomType.DECIMAL, ps.get(2).getAtomType());
        assertEquals("L2", as.get(2).getReferenceColumnName());

        assertEquals(
                tokenize("INSERT INTO D.S.T (C1, C2, C3) VALUES(",
                        ph(ps.get(0)), ", ", ph(ps.get(1)), ", ", ph(ps.get(2)), ")"),
                captureSource);

        assertEquals(List.of(Path.of("testing")), captureFiles);
    }

    @Test
    void testStyleError() throws Exception {
        var cols = table.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(table))
                    .errorOnCoflict()
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        assertEquals(1, ps.size());
        assertEquals(
                tokenize("INSERT INTO D.S.T (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);
    }

    @Test
    void testStyleSkip() throws Exception {
        var cols = table.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(table))
                    .skipOnCoflict()
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        assertEquals(1, ps.size());
        assertEquals(
                tokenize("INSERT IF NOT EXISTS INTO D.S.T (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);
    }

    @Test
    void testStyleOverwrite() throws Exception {
        var cols = table.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(table))
                    .overwriteOnCoflict()
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        assertEquals(1, ps.size());
        assertEquals(
                tokenize("UPDATE OR INSERT INTO D.S.T (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);
    }

    @Test
    void testMappingInconsistent() throws Exception {
        var cols = table.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(table))
                    .mapping(cols.get(0), "L0", String.class)
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        var as = captureParameters;
        assertEquals(1, ps.size());
        assertEquals(ps.size(), as.size());

        assertEquals(SqlCommon.AtomType.CHARACTER, ps.get(0).getAtomType());
        assertEquals("L0", as.get(0).getReferenceColumnName());

        assertEquals(
                tokenize("INSERT INTO D.S.T (C1) VALUES(CAST(", ph(ps.get(0)), " AS INT))"),
                captureSource);

        assertEquals(List.of(Path.of("testing")), captureFiles);
    }

    @Test
    void testDelimitedTable() throws Exception {
        var other = SqlResponse.DescribeTable.Success.newBuilder(table)
                .setDatabaseName("data base")
                .setTableName("My Table")
                .build();

        var cols = other.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(other))
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        assertEquals(
                tokenize("INSERT INTO \"data base\".S.\"My Table\" (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);
    }

    @Test
    void testDelimitedColumn() throws Exception {
        var other = SqlResponse.DescribeTable.Success.newBuilder(table)
                .clearColumns()
                .addColumns(SqlCommon.Column.newBuilder()
                        .setName("\"My\" Column")
                        .setAtomType(SqlCommon.AtomType.INT4))
                .build();

        var cols = other.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(other))
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        assertEquals(
                tokenize("INSERT INTO D.S.T (\"\"\"My\"\" Column\") VALUES(", ph(ps.get(0)), ")"),
                captureSource);

        assertEquals(List.of(Path.of("testing")), captureFiles);
    }

    @Test
    void testDatabaseEmpty() throws Exception {
        var other = SqlResponse.DescribeTable.Success.newBuilder(table)
                .clearDatabaseName()
                .build();
        var cols = other.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(other))
                    .errorOnCoflict()
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        assertEquals(1, ps.size());
        assertEquals(
                tokenize("INSERT INTO S.T (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);
    }

    @Test
    void testSchamaEmpty() throws Exception {
        var other = SqlResponse.DescribeTable.Success.newBuilder(table)
                .clearSchemaName()
                .build();
        var cols = other.getColumnsList();
        try (
            var load = LoadBuilder.loadTo(new TableMetadataAdapter(other))
                    .errorOnCoflict()
                    .mapping(cols.get(0), "L0")
                    .build(client).await();
        ) {
            load.submit(transaction, Path.of("testing")).await();
        }
        var ps = capturePlaceholders;
        assertEquals(1, ps.size());
        assertEquals(
                tokenize("INSERT INTO T (C1) VALUES(", ph(ps.get(0)), ")"),
                captureSource);
    }

    private static String ph(SqlRequest.PlaceHolder ps) {
        return String.format(":%s", ps.getName());
    }

    static List<String> tokenize(String... texts) {
        var results = new ArrayList<String>();
        for (var text : texts) {
            var matcher = PATTERN_TOKEN.matcher(text);
            int start = 0;
            while (start < text.length() && matcher.find(start)) {
                if (matcher.start() != start) {
                    throw new AssertionError(String.format("parse error:%n---%n%s%n---%nat %d", text, start));
                }
                start = matcher.end();
                if (matcher.group(1) != null) {
                    results.add(matcher.group(1));
//                } else {  // comment out for avoid checkstyle rule violation
//                    // ignore spaces
                }
            }
        }
        return results;
    }
}
