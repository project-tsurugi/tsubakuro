package com.tsurugidb.tsubakuro.examples.types;

import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class Main {
    private Main() {
    }

    private static String url = "ipc:tateyama";
//    private static String url = "tcp://localhost:12345/";

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("typesTest");
        System.out.println(tmpDir.toString());
        try (
                Session session = SessionBuilder.connect(url)
                .withCredential(new UsernamePasswordCredential("user", "pass"))
                .create(10, TimeUnit.SECONDS);
                SqlClient client = SqlClient.attach(session);) {

            clean(client);
            prepareData(client);
            Select.prepareAndSelect(client, "SELECT * FROM TTEMPORALS ORDER BY K0");
            Select.prepareAndSelect(client, "SELECT * FROM TDECIMALS ORDER BY K0");
        }
    }

    public static void prepareData(SqlClient client) throws Exception {
        // insert initial data
        try (
                var prep = client.prepare(
                        "INSERT INTO TTEMPORALS(K0, K1, K2, K3, K4, C0, C1, C2, C3, C4) VALUES(:p0, :p1, :p2, :p3, :p4, :p0, :p1, :p2, :p3, :p4)",
                        Placeholders.of("p0", LocalDate.class),
                        Placeholders.of("p1", LocalTime.class),
                        Placeholders.of("p2", OffsetTime.class),
                        Placeholders.of("p3", LocalDateTime.class),
                        Placeholders.of("p4", OffsetDateTime.class)
                ).await();
                Transaction tx = client.createTransaction().await()
        ) {
            for (int i = 0; i < 10; ++i) {
                tx.executeStatement(
                        prep,
                        List.of(
                                Parameters.of("p0", LocalDate.of(2000, 1, 1 + i)),
                                Parameters.of("p1", LocalTime.of(12, 0, i)),
                                Parameters.of("p2", OffsetTime.of(12, 0, i, 0, ZoneOffset.UTC)),
                                Parameters.of("p3", LocalDateTime.of(2000, 1, 1 + i, 12, 0, i)),
                                Parameters.of("p4", OffsetDateTime.of(2000, 1, 1 + i, 12, 0, i, 0, ZoneOffset.UTC))
                        )
                ).await();
            }
            tx.commit().await();
        }
        try (
                var prep = client.prepare(
                        "INSERT INTO TDECIMALS(K0, K1, K2, C0, C1, C2) VALUES(:p0, :p1, :p2, :p0, :p1, :p2)",
                        Placeholders.of("p0", BigDecimal.class),
                        Placeholders.of("p1", BigDecimal.class),
                        Placeholders.of("p2", BigDecimal.class)
                        ).await();
                Transaction tx = client.createTransaction().await()
        ) {
            for (int i = 0; i < 10; ++i) {
                tx.executeStatement(
                        prep,
                        List.of(
                                Parameters.of("p0", BigDecimal.valueOf(111 + i)),
                                Parameters.of("p1", BigDecimal.valueOf(11.111 + i)),
                                Parameters.of("p2", BigDecimal.valueOf(11111.1 + i))
                                )
                ).await();
            }
            tx.commit().await();
        }
    }

    public static void clean(SqlClient client) throws Exception {
        try (
                var prep = client.prepare("DELETE FROM TTEMPORALS").await();
                Transaction tx = client.createTransaction().await()
        ) {
            tx.executeStatement(prep).await();
            tx.commit().await();
        }
        try (
                var prep = client.prepare("DELETE FROM TDECIMALS").await();
                Transaction tx = client.createTransaction().await()
        ) {
            tx.executeStatement(prep).await();
            tx.commit().await();
        }
    }
}
