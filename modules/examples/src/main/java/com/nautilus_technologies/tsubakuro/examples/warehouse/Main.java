package com.nautilus_technologies.tsubakuro.low.warehouse;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;

public final class Main {
    private Main() {
    }
    
    private static String url = "ipc:tateyama";
    //    private static String url = "tcp://localhost:12345/";

    public static void main(String[] args) {
        try (
            Session session = SessionBuilder.connect(url)
            .withCredential(new UsernamePasswordCredential("user", "pass"))
            .create(10, TimeUnit.SECONDS);
            SqlClient sqlClient = SqlClient.attach(session);) {

            new Select(sqlClient).select();

        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            System.out.println(e);
        }
    }
}