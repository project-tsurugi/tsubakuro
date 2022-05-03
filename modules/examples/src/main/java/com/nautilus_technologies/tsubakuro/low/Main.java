package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.channel.common.connection.ConnectorImpl;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;

public final class Main {
    private Main() {
    }

    private static String dbName = "tateyama";

    public static void main(String[] args) {
        try {
            (new Insert(new ConnectorImpl(dbName), new SessionImpl())).prepareAndInsert();
            (new Select(new ConnectorImpl(dbName), new SessionImpl())).prepareAndSelect();
        } catch (IOException | ServerException | InterruptedException e) {
            System.out.println(e);
        }
    }
}
