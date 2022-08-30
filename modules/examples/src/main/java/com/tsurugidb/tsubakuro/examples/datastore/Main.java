package com.tsurugidb.tsubakuro.examples.datastore;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.datastore.DatastoreClient;
import com.tsurugidb.tsubakuro.datastore.Backup;

public final class Main {
    private Main() {
    }

    private static String url = "ipc:tateyama";
    //    private static String url = "tcp://localhost:12345/";

    public static void main(String[] args) throws Exception {

        try (
                Session session = SessionBuilder.connect(url)
                .withCredential(new UsernamePasswordCredential("user", "pass"))
                .create(10, TimeUnit.SECONDS);
                DatastoreClient client = DatastoreClient.attach(session);) {

            try (Backup backup = client.beginBackup().await()) {
                System.out.println("files are:");
                for (Path source : backup.getFiles()) {
                    System.out.println(source);
                }
                backup.keepAlive(1, TimeUnit.SECONDS);
                backup.close();
            }
        }
    }
}
