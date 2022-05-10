package com.nautilus_technologies.tsubakuro.low.dumpload;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

public final class Main {
    private Main() {
    }

    //    private static String url = "ipc:tateyama";
    private static String url = "tcp://localhost:12345/";

    public static void main(String[] args) throws Exception {

        try (
                Session session = SessionBuilder.connect(url)
                .withCredential(new UsernamePasswordCredential("user", "pass"))
                .create(10, TimeUnit.SECONDS);
                SqlClient client = SqlClient.attach(session);) {

            try (Transaction transaction = client.createTransaction().await()) {
                // create table
                var responseCreateTable = transaction.executeStatement("CREATE TABLE dump_load_test(pk INT PRIMARY KEY, c1 INT)").await();
                if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(responseCreateTable.getResultCase())) {
                    throw new IOException("error in create table");
                }
                if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.commit().get().getResultCase())) {
                    throw new IOException("error in commit");
                }
            }

            // load
            try (
                    var prep = client.prepare(
                            "INSERT INTO dump_load_test(pk, c1) VALUES(:pk, :c1)",
                            Placeholders.of("pk", int.class),
                            Placeholders.of("c1", int.class)).await();
                    Transaction tx = client.createTransaction().await()
            ) {
                var result = tx.executeLoad(
                        prep,
                        List.of(),
                        Path.of("/path/to/load-parameter"))
                        .await();
                if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(result.getResultCase())) {
                    throw new IOException("error executeLoad");
                }
                var status = tx.commit().await();
                if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(status.getResultCase())) {
                    throw new IOException("error in commit");
                }
            }

            try (
                    var prep = client.prepare("SELECT * FROM dump_load_test").await();
                    var tx = client.createTransaction().await();
                    var results = tx.executeDump(prep, List.of(), Path.of("/path/to/dump-target")).await();
            ) {
                while (results.nextRecord()) {
                    while (results.nextColumn()) {
                        var s = results.getCharacter();
                        System.out.println(s);
                    }
                }
                var status = tx.commit().await();
                if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(status.getResultCase())) {
                    throw new IOException("error in commit");
                }
            }
        }
    }
}
