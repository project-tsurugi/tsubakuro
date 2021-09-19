package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;

public final class Main {
    static long warehouses()  throws IOException, ExecutionException, InterruptedException {
	var connector = new IpcConnectorImpl(dbName);
	var session = new SessionImpl();
	session.connect(connector.connect().get());
	
	var transaction = session.createTransaction().get();
	var resultSet = transaction.executeQuery("SELECT COUNT(w_id) FROM WAREHOUSE").get();
	long count = 0;
	if (resultSet.nextRecord()) {
	    if (resultSet.nextColumn()) {
		count = resultSet.getInt8();
	    }
	}
	transaction.commit().get();
	session.close();
	return count;
    }

    private Main() {
    }
    
    static String dbName = "tateyama";
    static int threads = 2;
    static int duration = 10;
    
    public static void main(String[] args) {
	ArrayList<Client> clients = new ArrayList<>();
	ArrayList<Profile> profiles = new ArrayList<>();
	CyclicBarrier barrier = new CyclicBarrier(threads + 1);
	AtomicBoolean stop = new AtomicBoolean();
	AtomicBoolean[] doingDelivery = new AtomicBoolean[threads];

	try {
	    var warehouses = warehouses();
	
	    for (int i = 0; i < threads; i++) {
		doingDelivery[i].set(false);
		var profile = new Profile();
		profile.warehouses = warehouses;
		profile.index = i;

		profiles.add(profile);
		clients.add(new Client(new IpcConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop, doingDelivery));
	    }
	    
	    long start = System.currentTimeMillis();
	    for (int i = 0; i < clients.size(); i++) {
		clients.get(i).start();
	    }
	    Thread.sleep(duration * 1000);
	    stop.set(true);
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
