package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;

public class  Client extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    DeferredHelper doingDelivery;
    RandomGenerator randomGenerator;
    Profile profile;
    String url;
    NewOrder newOrder;
    Payment payment;
    Delivery delivery;
    OrderStatus orderStatus;
    StockLevel stockLevel;

    public  Client(String url, Profile profile, CyclicBarrier barrier, AtomicBoolean stop, DeferredHelper doingDelivery) throws IOException, ServerException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.doingDelivery = doingDelivery;
        this.randomGenerator = new RandomGenerator();
        this.profile = profile;
        this.url = url;
    
        prepare();
    }

    void prepare()  throws IOException, ServerException, InterruptedException {
        newOrder.prepare();
        payment.prepare();
        delivery.prepare();
        orderStatus.prepare();
        stockLevel.prepare();
    }

    public void run() {
    int pendingDelivery = 0;
    int wId;

    try (
        Session session = SessionBuilder.connect(url)
        .withCredential(new UsernamePasswordCredential("user", "pass"))
        .create(10, TimeUnit.SECONDS);
        SqlClient sqlClient = SqlClient.attach(session);) {

        newOrder = new NewOrder(sqlClient, randomGenerator, profile);
        payment = new Payment(sqlClient, randomGenerator, profile);
        delivery = new Delivery(sqlClient, randomGenerator, profile);
        orderStatus = new OrderStatus(sqlClient, randomGenerator, profile);
        stockLevel = new StockLevel(sqlClient, randomGenerator, profile);

        barrier.await();
        long start = System.currentTimeMillis();

        while (!stop.get()) {
        long transactionStart = System.nanoTime();
        if (pendingDelivery > 0) {
            wId = (int) delivery.warehouseId();
            if (!doingDelivery.getAndSet(wId - 1, true)) {
            delivery.transaction(stop);
            profile.time.delivery += (System.nanoTime() - transactionStart);
            doingDelivery.set(wId - 1, false);
            pendingDelivery--;
            if (pendingDelivery > 0) {
                delivery.setParams();
            }
            continue;
            }
        }

        var transactionType = randomGenerator.uniformWithin(1, 100);
        if (transactionType <= Percent.KXCT_NEWORDER_PERCENT) {
            newOrder.setParams();
            newOrder.transaction(stop);
            profile.time.newOrder += (System.nanoTime() - transactionStart);
        } else if (transactionType <= Percent.KXCT_PAYMENT_PERCENT) {
            payment.setParams();
            payment.transaction(stop);
            profile.time.payment += (System.nanoTime() - transactionStart);
        } else if (transactionType <= Percent.KXCT_ORDERSTATUS_PERCENT) {
            orderStatus.setParams();
            orderStatus.transaction(stop);
            profile.time.orderStatus += (System.nanoTime() - transactionStart);
        } else if (transactionType <= Percent.KXCT_DELIEVERY_PERCENT) {
            if (pendingDelivery > 0) {
            pendingDelivery++;
            continue;
            }
            delivery.setParams();
            wId = (int) delivery.warehouseId();
            if (!doingDelivery.getAndSet(wId - 1, true)) {
            delivery.transaction(stop);
            profile.time.delivery += (System.nanoTime() - transactionStart);
            doingDelivery.set(wId - 1, false);
            } else {
            pendingDelivery++;
            }
        } else {
            stockLevel.setParams();
            stockLevel.transaction(stop);
            profile.time.stockLevel += (System.nanoTime() - transactionStart);
        }
        }
        profile.elapsed = System.currentTimeMillis() - start;
        try {
        session.close();
        } catch (IOException e) {
        System.out.println(e);
        }
    } catch (IOException | ServerException | InterruptedException | BrokenBarrierException | TimeoutException e) {
        System.out.println(e);
        e.printStackTrace();
        }
    }
}
