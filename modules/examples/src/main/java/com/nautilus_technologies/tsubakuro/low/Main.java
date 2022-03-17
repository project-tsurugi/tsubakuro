package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.channel.ipc.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;

public final class Main {
    private Main() {
    }
    
    private static String dbName = "tateyama";
    
    public static void main(String[] args) {
	try {
	    (new Insert(new IpcConnectorImpl(dbName), new SessionImpl())).prepareAndInsert();
	    (new Select(new IpcConnectorImpl(dbName), new SessionImpl())).prepareAndSelect();
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}
