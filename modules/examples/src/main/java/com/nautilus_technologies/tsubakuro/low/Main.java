package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;

public final class Main {
    private Main() {
    }
    
    private static String dbName = "tsubakuro";
    
    public static void main(String[] args) {
	try {
	    Connector connector = new IpcConnectorImpl(dbName);
	    Session session = new SessionImpl();

	    Select select = new Select(connector, session);
	    select.select(args[0]);
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}
