package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;

public class  Client extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    DeferredHelper doingDelivery;
    Session session;
    RandomGenerator randomGenerator;
    Profile profile;
    NewOrder newOrder;
    Payment payment;
    Delivery delivery;
    OrderStatus orderStatus;
    StockLevel stockLevel;

    public  Client(Connector connector, Session session, Profile profile, CyclicBarrier barrier, AtomicBoolean stop, DeferredHelper doingDelivery) throws IOException, ServerException, InterruptedException {
	this.barrier = barrier;
	this.stop = stop;
	this.doingDelivery = doingDelivery;
	this.session = session;
	this.session.connect(connector.connect().get());
	this.randomGenerator = new RandomGenerator();
	this.profile = profile;

	this.newOrder = new NewOrder(session, randomGenerator, profile);
	this.payment = new Payment(session, randomGenerator, profile);
	this.delivery = new Delivery(session, randomGenerator, profile);
	this.orderStatus = new OrderStatus(session, randomGenerator, profile);
	this.stockLevel = new StockLevel(session, randomGenerator, profile);
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

	try {
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
	} catch (IOException | ServerException | InterruptedException | BrokenBarrierException e) {
	    System.out.println(e);
	    e.printStackTrace();
        }
    }
}
