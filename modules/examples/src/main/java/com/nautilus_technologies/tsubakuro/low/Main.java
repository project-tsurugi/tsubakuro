package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;

public final class Main {
    private Main() {
    }
    
    private static String dbName = "tsubakuro";
    
    public static void main(String[] args) {
	try {
	    (new Insert(new IpcConnectorImpl(dbName), new SessionImpl())).insert();
	    (new Select(new IpcConnectorImpl(dbName), new SessionImpl())).select();
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}