package com.nautilus_technologies.tsubakuro.examples.datastore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreClient;
import com.nautilus_technologies.tsubakuro.low.datastore.Backup;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;

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
