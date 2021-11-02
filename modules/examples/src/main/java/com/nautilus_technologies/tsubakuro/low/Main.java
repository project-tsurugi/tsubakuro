package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
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
    
    private static String dbName = "tateyama";
    static int pattern = 1;
    static int duration = 30;

    public static void main(String[] args) {
        int argl = args.length;
        if (argl > 0) {
            pattern = Integer.parseInt(args[0]);
            if (argl > 1) {
                duration = Integer.parseInt(args[1]);
            }
        }

        try {
            var warehouses = warehouses();
            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicBoolean stop = new AtomicBoolean();
	    var profile = new Profile(warehouses);

	    //	    var client = new Select(new IpcConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop);
	    var client = new Insert(new IpcConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop);
	    client.start();
            barrier.await();
            System.out.println("benchmark started, warehouse = " + warehouses);
            Thread.sleep(duration * 1000);
            stop.set(true);
            System.out.println("benchmark stoped");
	    profile.print();

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
