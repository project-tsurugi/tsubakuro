package com.nautilus_technologies.tsubakuro.examples.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.low.backup.DatastoreClient;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

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

            try (Backup transaction = client.beginBackup().await()) {
                
            }
        }
    }
}
