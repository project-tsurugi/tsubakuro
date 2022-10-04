package com.tsurugidb.tsubakuro.examples.dumpload;

import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.util.LoadBuilder;
import com.tsurugidb.sql.proto.SqlRequest;
import org.apache.commons.cli.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class Main {
    private Main() {
    }

    private static String url = "ipc:tateyama";
//    private static String url = "tcp://localhost:12345/";

    public static void main(String[] args) throws Exception {
        boolean buildStatement = false;
        boolean prepareTables = false;
        long recordsPerFile = 0;
        boolean keepFilesOnError = false;
        Options options = new Options();
        options.addOption(Option.builder("s").argName("statement_builder").desc("Use statement utility for load.").build());
        options.addOption(Option.builder("t").argName("prepare_tables").desc("Create tables and prepare data for dump/load test.").build());
        options.addOption(Option.builder("r").argName("records_per_file").hasArg().desc("Specify the maximum records count per file.").build());
        options.addOption(Option.builder("k").argName("keep_files_on_error").desc("Keep the dump output files even when dump failed.").build());
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("s")) {
                buildStatement = true;
                System.out.println("use statement utility");
            }
            if (cmd.hasOption("t")) {
                prepareTables = true;
                System.out.println("create table for testing");
            }
            if (cmd.hasOption("r")) {
                recordsPerFile = Long.parseLong(cmd.getOptionValue("r"));
                System.out.println("max records per file set : " + recordsPerFile);
            }
            if (cmd.hasOption("k")) {
                keepFilesOnError = true;
                System.out.println("will keep files on error");
            }
        } catch (ParseException e) {
            System.err.printf("cmd parser failed." + e);
        }

        Path tmpDir = Files.createTempDirectory("dumpLoadTest");
        System.out.println(tmpDir.toString());
        try (
                Session session = SessionBuilder.connect(url)
                .withCredential(new UsernamePasswordCredential("user", "pass"))
                .create(10, TimeUnit.SECONDS);
                SqlClient client = SqlClient.attach(session);) {

            if (prepareTables) {
                prepareData(client);
            }

            // dump
            var files = new ArrayList<Path>();
            var option = SqlRequest.DumpOption.newBuilder()
                    .setMaxRecordCountPerFile(recordsPerFile)
                    .setFailBehaviorValue(keepFilesOnError
                            ? SqlRequest.DumpFailBehavior.KEEP_FILES_VALUE
                            : SqlRequest.DumpFailBehavior.DELETE_FILES_VALUE)
                    .build();
            try (
                    var prep = client.prepare("SELECT * FROM dump_source").await();
                    var tx = client.createTransaction().await();
                    var results = tx.executeDump(prep, List.of(), Path.of(tmpDir.toString()), option).await();
            ) {
                while (results.nextRow()) {
                    while (results.nextColumn()) {
                        var s = results.fetchCharacterValue();
                        System.out.println(s);
                        files.add(Path.of(s));
                    }
                }
                tx.commit().await();
            }

            // clean target table
            clean(client);

            // load
            if (buildStatement) {
                try (
                        Transaction tx = client.createTransaction().await()
                ) {
                    var f = client.getTableMetadata("load_target");
                    var meta = f.await();

                    var cols = meta.getColumns();
                    var l = LoadBuilder.loadTo(meta)
                            .mapping(cols.get(0), 0)
                            .mapping(cols.get(1), 1)
                            .mapping(cols.get(2), 2)
                            .mapping(cols.get(3), 3)
                            .mapping(cols.get(4), 4)
                            .mapping(cols.get(5), 5)
                            .mapping(cols.get(6), 6)
                            .mapping(cols.get(7), 7)
                            .mapping(cols.get(8), 8)
                            .mapping(cols.get(9), 9)
                            .mapping(cols.get(10), 10)
                            .mapping(cols.get(11), 11)
                            .mapping(cols.get(12), 12)
                            .errorOnCoflict().style(LoadBuilder.Style.ERROR).build(client);
                    var load = l.await();
                    load.submit(tx, files).get();
                    tx.commit().await();
                }
            } else {
                try (
                        var prep = client.prepare(
                                "INSERT INTO load_target(pk, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) VALUES(:p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12)",
                                Placeholders.of("p0", int.class),
                                Placeholders.of("p1", int.class),
                                Placeholders.of("p2", long.class),
                                Placeholders.of("p3", float.class),
                                Placeholders.of("p4", double.class),
                                Placeholders.of("p5", String.class),
                                Placeholders.of("p6", LocalDate.class),
                                Placeholders.of("p7", LocalTime.class),
                                Placeholders.of("p8", OffsetTime.class),
                                Placeholders.of("p9", LocalDateTime.class),
                                Placeholders.of("p10", OffsetDateTime.class),
                                Placeholders.of("p11", BigDecimal.class),
                                Placeholders.of("p12", BigDecimal.class)
                        ).await();
                        Transaction tx = client.createTransaction().await()
                ) {
                    tx.executeLoad(
                                    prep,
                                    List.of(
                                            Parameters.referenceColumn("p0", "pk"),
                                            Parameters.referenceColumn("p1", "c1"),
                                            Parameters.referenceColumn("p2", "c2"),
                                            Parameters.referenceColumn("p3", "c3"),
                                            Parameters.referenceColumn("p4", "c4"),
                                            Parameters.referenceColumn("p5", "c5"),
                                            Parameters.referenceColumn("p6", "c6"),
                                            Parameters.referenceColumn("p7", "c7"),
                                            Parameters.referenceColumn("p8", "c8"),
                                            Parameters.referenceColumn("p9", "c9"),
                                            Parameters.referenceColumn("p10", "c10"),
                                            Parameters.referenceColumn("p11", "c11"),
                                            Parameters.referenceColumn("p12", "c12")
                                    ),
                                    files)
                            .await();
                    tx.commit().await();
                }
            }

            // verify
            Select.prepareAndSelect(client, "SELECT * FROM load_target ORDER BY pk");
        }
    }

    public static void prepareData(SqlClient client) throws Exception {
        try (Transaction transaction = client.createTransaction().await()) {
            // create table
            transaction.executeStatement("CREATE TABLE dump_source (" +
                    "pk INT PRIMARY KEY, " +
                    "c1 INT, " +
                    "c2 BIGINT, " +
                    "c3 FLOAT, " +
                    "c4 DOUBLE, " +
                    "c5 VARCHAR(10), " +
                    "c6 DATE, " +
                    "c7 TIME," +
                    "c8 TIME WITH TIME ZONE," +
                    "c9 TIMESTAMP," +
                    "c10 TIMESTAMP WITH TIME ZONE," +
                    "c11 DECIMAL(5,3)," +
                    "c12 DECIMAL(10)" +
                    ")").await();
            transaction.commit().get();
        }

        try (Transaction transaction = client.createTransaction().await()) {
            // create table
            transaction.executeStatement("CREATE TABLE load_target (" +
                    "pk INT PRIMARY KEY, " +
                    "c1 INT, " +
                    "c2 BIGINT, " +
                    "c3 FLOAT, " +
                    "c4 DOUBLE, " +
                    "c5 VARCHAR(10), " +
                    "c6 DATE, " +
                    "c7 TIME," +
                    "c8 TIME WITH TIME ZONE," +
                    "c9 TIMESTAMP," +
                    "c10 TIMESTAMP WITH TIME ZONE," +
                    "c11 DECIMAL(5,3)," +
                    "c12 DECIMAL(10)" +
                    ")").await();
            transaction.commit().get();
        }

        // insert initial data
        try (
                var prep = client.prepare(
                        "INSERT INTO dump_source(pk, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) VALUES(:p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10, :p11, :p12)",
                        Placeholders.of("p0", int.class),
                        Placeholders.of("p1", int.class),
                        Placeholders.of("p2", long.class),
                        Placeholders.of("p3", float.class),
                        Placeholders.of("p4", double.class),
                        Placeholders.of("p5", String.class),
                        Placeholders.of("p6", LocalDate.class),
                        Placeholders.of("p7", LocalTime.class),
                        Placeholders.of("p8", OffsetTime.class),
                        Placeholders.of("p9", LocalDateTime.class),
                        Placeholders.of("p10", OffsetDateTime.class),
                        Placeholders.of("p11", BigDecimal.class),
                        Placeholders.of("p12", BigDecimal.class)
                ).await();
                Transaction tx = client.createTransaction().await()
        ) {
            for (int i = 0; i < 10; ++i) {
                tx.executeStatement(
                        prep,
                        List.of(
                                Parameters.of("p0", i),
                                Parameters.of("p1", 10 * i),
                                Parameters.of("p2", 100L * i),
                                Parameters.of("p3", 1000.0f * i),
                                Parameters.of("p4", 10000.0 * i),
                                Parameters.of("p5", String.valueOf(100000 * i)),
                                Parameters.of("p6", LocalDate.of(2000, 1, 1+i)),
                                Parameters.of("p7", LocalTime.of(12, 0, i)),
                                Parameters.of("p8", OffsetTime.of(12, 0, i, 0, ZoneOffset.UTC)),
                                Parameters.of("p9", LocalDateTime.of(2000, 1, 1+i, 12, 0, i, 0)),
                                Parameters.of("p10", OffsetDateTime.of(2000, 1, 1+i, 12, 0, i, 0, ZoneOffset.UTC)),
                                Parameters.of("p11", BigDecimal.valueOf(1000+i, 3)),
                                Parameters.of("p12", BigDecimal.valueOf(1000+i))
                        )
                ).await();
            }
            tx.commit().await();
        }
    }

    public static void clean(SqlClient client) throws Exception {
        try (
                var prep = client.prepare("DELETE FROM load_target").await();
                Transaction tx = client.createTransaction().await()
        ) {
            tx.executeStatement(prep).await();
            tx.commit().await();
        }
    }
}
