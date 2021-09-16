package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.tpcc.TpccClient;

public final class Main {
    private Main() {
    }
    
    private static String dbName = "tateyama";
    
    public static void main(String[] args) {
	ArrayList<TpccClient> clients = new ArrayList<>();
	try {
	    for (int i = 0; i < 2; i++) {
		clients.add(new TpccClient(new IpcConnectorImpl(dbName), new SessionImpl()));
	    }
	    
	    long start = System.currentTimeMillis();
	    for (int i = 0; i < clients.size(); i++) {
		clients.get(i).start();
	    }
	    for (int i = 0; i < clients.size(); i++) {
		clients.get(i).join();
	    }
	    System.out.printf("elapsed: %d ms%n", System.currentTimeMillis() - start);
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}
