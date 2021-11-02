package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

public final class Main {
    static long warehouses()  throws IOException, ExecutionException, InterruptedException {
	var connector = new IpcConnectorImpl(dbName);
	var session = new SessionImpl();
	session.connect(connector.connect().get());

	var transaction = session.createTransaction().get();
	var future = transaction.executeQuery("SELECT COUNT(w_id) FROM WAREHOUSE");
	var resultSet = future.getLeft().get();
	long count = 0;
	if (resultSet.nextRecord()) {
	    if (resultSet.nextColumn()) {
		count = resultSet.getInt8();
	    }
	}
	resultSet.close();
	if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future.getRight().get().getResultCase())) {
	    throw new IOException("select error");
	}
	transaction.commit().get();
	session.close();
	return count;
    }

    private Main() {
    }

    static String dbName = "tateyama";
    static int threads = 8;
    static int duration = 30;

    public static void main(String[] args) {
        int argl = args.length;
	boolean fixThreadMapping = false;
	if (argl > 0) {
            threads = Integer.parseInt(args[0]);
	    if (threads < 0) {
		threads = -threads;
		fixThreadMapping = true;
	    }
            if (argl > 1) {
                duration = Integer.parseInt(args[1]);
            }
        }

	try {
	    var warehouses = warehouses();
	    if (fixThreadMapping) {
		if (threads != warehouses) {
		    System.out.printf("threads %d, warehouses %d\n", threads, warehouses);
		    return;
		}
		System.out.println("fixThreadMapping is true");
	    }
	    AtomicBoolean[] doingDelivery = new AtomicBoolean[(int) warehouses];
	    for (int i = 0; i < warehouses; i++) {
		doingDelivery[i] = new AtomicBoolean(false);
	    }

	    ArrayList<Client> clients = new ArrayList<>();
	    ArrayList<Profile> profiles = new ArrayList<>();
	    CyclicBarrier barrier = new CyclicBarrier(threads + 1);
	    AtomicBoolean stop = new AtomicBoolean();

	    for (int i = 0; i < threads; i++) {
		var profile = new Profile();
		profile.warehouses = warehouses;
		profile.threads = threads;
		profile.index = i;
		profile.fixThreadMapping = fixThreadMapping;
		profiles.add(profile);
		clients.add(new Client(new IpcConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop, doingDelivery));
	    }

	    long start = System.currentTimeMillis();
	    for (int i = 0; i < clients.size(); i++) {
		clients.get(i).start();
	    }
	    barrier.await();
	    System.out.println("benchmark started, warehouse = " + warehouses + ", threads = " + threads);
	    Thread.sleep(duration * 1000);
	    stop.set(true);
	    System.out.println("benchmark stoped");
	    var total = new Profile();
	    for (int i = 0; i < clients.size(); i++) {
		clients.get(i).join();
		total.add(profiles.get(i));
	    }
	    total.print(threads);
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
	} catch (BrokenBarrierException e) {
	    System.out.println(e);
	}
    }
}
