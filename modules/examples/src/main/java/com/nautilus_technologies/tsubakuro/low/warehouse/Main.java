package com.nautilus_technologies.tsubakuro.low.warehouse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.channel.common.connection.ConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;

public final class Main {
    private Main() {
    }
    
    private static String dbName = "tateyama";
    
    public static void main(String[] args) {
	try {
	    (new Select(new ConnectorImpl("localhost:12345"), new SessionImpl())).select();  // for stream channel
	    //	    (new Select(new ConnectorImpl("tateyama"), new SessionImpl())).select();  // for ipc channel
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}
