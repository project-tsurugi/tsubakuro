package com.nautilus_technologies.tsubakuro.low.warehouse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;

public final class Main {
    private Main() {
    }
    
    //    private static String url = "ipc:tateyama";
    private static String url = "tcp://localhost:12345/";

    public static void main(String[] args) {
        try {
            Session session = new SessionImpl();
            session.connect(Connector.create(url).connect().get());
            new Select(session).select();  // for stream channel
            //	    (new Select(new ConnectorImpl("tateyama"), new SessionImpl())).select();  // for ipc channel
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.out.println(e);
        }
    }
}
