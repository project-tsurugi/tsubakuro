package com.nautilus_technologies.tsubakuro.examples.dumpload;

import  com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.util.LoadBuilder;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
        Options options = new Options();
        options.addOption(Option.builder("s").argName("statement_builder").desc("Use statement utility for load.").build());
        options.addOption(Option.builder("t").argName("prepare_tables").desc("Create tables and prepare data for dump/load test.").build());
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
            try (
                    var prep = client.prepare("SELECT * FROM dump_source").await();
                    var tx = client.createTransaction().await();
                    var results = tx.executeDump(prep, List.of(), Path.of(tmpDir.toString())).await();
            ) {
                while (results.nextRecord()) {
                    while (results.nextColumn()) {
                        var s = results.getCharacter();
                        System.out.println(s);
                        files.add(Path.of(s));
                    }
                }
                var status = tx.commit().await();
                if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(status.getResultCase())) {
                    throw new IOException("error in commit");
                }
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
                            .errorOnCoflict().style(LoadBuilder.Style.ERROR).build(client);
                    var load = l.await();

                    var result = load.submit(tx, files).get();

                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(result.getResultCase())) {
                        throw new IOException("error loading");
                    }
                    var status = tx.commit().await();
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(status.getResultCase())) {
                        throw new IOException("error in commit");
                    }
                }
            } else {
                try (
                        var prep = client.prepare(
                                "INSERT INTO load_target(pk, c1, c2, c3, c4, c5) VALUES(:p0, :p1, :p2, :p3, :p4, :p5)",
                                Placeholders.of("p0", int.class),
                                Placeholders.of("p1", int.class),
                                Placeholders.of("p2", long.class),
                                Placeholders.of("p3", float.class),
                                Placeholders.of("p4", double.class),
                                Placeholders.of("p5", String.class)
                        ).await();
                        Transaction tx = client.createTransaction().await()
                ) {
                    var result = tx.executeLoad(
                                    prep,
                                    List.of(
                                            Parameters.referenceColumn("p0", "pk"),
                                            Parameters.referenceColumn("p1", "c1"),
                                            Parameters.referenceColumn("p2", "c2"),
                                            Parameters.referenceColumn("p3", "c3"),
                                            Parameters.referenceColumn("p4", "c4"),
                                            Parameters.referenceColumn("p5", "c5")
                                    ),
                                    files)
                            .await();
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(result.getResultCase())) {
                        throw new IOException("error executeLoad");
                    }
                    var status = tx.commit().await();
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(status.getResultCase())) {
                        throw new IOException("error in commit");
                    }
                }
            }

            // verify
            Select.prepareAndSelect(client, "SELECT * FROM load_target ORDER BY pk");
        }
    }

    public static void prepareData(SqlClient client) throws Exception {
        try (Transaction transaction = client.createTransaction().await()) {
            // create table
            var responseCreateTable = transaction.executeStatement("CREATE TABLE dump_source (pk INT PRIMARY KEY, c1 INT, c2 BIGINT, c3 FLOAT, c4 DOUBLE, c5 VARCHAR(10))").await();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(responseCreateTable.getResultCase())) {
                throw new IOException("error in create table");
            }
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(transaction.commit().get().getResultCase())) {
                throw new IOException("error in commit");
            }
        }

        try (Transaction transaction = client.createTransaction().await()) {
            // create table
            var responseCreateTable = transaction.executeStatement("CREATE TABLE load_target (pk INT PRIMARY KEY, c1 INT, c2 BIGINT, c3 FLOAT, c4 DOUBLE, c5 VARCHAR(10))").await();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(responseCreateTable.getResultCase())) {
                throw new IOException("error in create table");
            }
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(transaction.commit().get().getResultCase())) {
                throw new IOException("error in commit");
            }
        }

        // insert initial data
        try (
                var prep = client.prepare(
                        "INSERT INTO dump_source(pk, c1, c2, c3, c4, c5) VALUES(:p0, :p1, :p2, :p3, :p4, :p5)",
                        Placeholders.of("p0", int.class),
                        Placeholders.of("p1", int.class),
                        Placeholders.of("p2", long.class),
                        Placeholders.of("p3", float.class),
                        Placeholders.of("p4", double.class),
                        Placeholders.of("p5", String.class)
                ).await();
                Transaction tx = client.createTransaction().await()
        ) {
            for (int i = 0; i < 10; ++i) {
                var result = tx.executeStatement(
                        prep,
                        List.of(
                                Parameters.of("p0", i),
                                Parameters.of("p1", 10 * i),
                                Parameters.of("p2", 100L * i),
                                Parameters.of("p3", 1000.0f * i),
                                Parameters.of("p4", 10000.0 * i),
                                Parameters.of("p5", String.valueOf(100000 * i))
                        )
                ).await();
                if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(result.getResultCase())) {
                    throw new IOException("error executeStatement");
                }
            }
            var status = tx.commit().await();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(status.getResultCase())) {
                throw new IOException("error in commit");
            }
        }
    }

    public static void clean(SqlClient client) throws Exception {
        try (
                var prep = client.prepare("DELETE FROM load_target").await();
                Transaction tx = client.createTransaction().await()
        ) {
            var result = tx.executeStatement(prep).await();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(result.getResultCase())) {
                throw new IOException("error executeStatement");
            }
            var status = tx.commit().await();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(status.getResultCase())) {
                throw new IOException("error in commit");
            }
        }
    }
}
