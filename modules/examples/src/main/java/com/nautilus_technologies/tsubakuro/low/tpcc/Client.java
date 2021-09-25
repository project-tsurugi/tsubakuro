package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;

public class  Client extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    AtomicBoolean[] doingDelivery;
    Session session;
    RandomGenerator randomGenerator;
    Profile profile;
    NewOrder newOrder;
    Payment payment;
    Delivery delivery;
    OrderStatus orderStatus;
    StockLevel stockLevel;

    public  Client(Connector connector, Session session, Profile profile, CyclicBarrier barrier, AtomicBoolean stop, AtomicBoolean[] doingDelivery) throws IOException, ExecutionException, InterruptedException {
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

    void prepare()  throws IOException, ExecutionException, InterruptedException {
	newOrder.prepare();
	payment.prepare();
	delivery.prepare();
	orderStatus.prepare();
	stockLevel.prepare();
    }

    Transaction createTransaction() throws IOException, InterruptedException, ExecutionException {
	var transaction = session.createTransaction().get();
	return transaction;
    }

    public void run() {
	int pendingDelivery = 0;
	int wId;

	try {
	    barrier.await();
	    long start = System.currentTimeMillis();

	    while (!stop.get()) {
		if (pendingDelivery > 0) {
		    wId = (int) delivery.warehouseId();
		    if (!doingDelivery[wId - 1].getAndSet(true)) {
			while (!stop.get()) {
			    var transaction = createTransaction();
			    try {
				delivery.transaction(transaction);
				doingDelivery[wId - 1].set(false);
				pendingDelivery--;
				if (pendingDelivery > 0) {
				    delivery.setParams();
				}
				break;
			    } catch (IOException e) {
				e.printStackTrace();
				transaction.rollback();
				profile.retry.delivery++;
			    }
			}
			continue;
		    }
		}

		var transactionType = randomGenerator.uniformWithin(1, 100);
		if (transactionType <= Percent.KXCT_NEWORDER_PERCENT) {
		    newOrder.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    newOrder.transaction(transaction);
			    break;
			} catch (IOException e) {
			    e.printStackTrace();
			    transaction.rollback();
			    profile.retry.newOrder++;
			}
		    }
		} else if (transactionType <= Percent.KXCT_PAYMENT_PERCENT) {
		    payment.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    payment.transaction(transaction);
			    break;
			} catch (IOException e) {
			    e.printStackTrace();
			    transaction.rollback();
			    profile.retry.payment++;
			}
		    }
		} else if (transactionType <= Percent.KXCT_ORDERSTATUS_PERCENT) {
		    orderStatus.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    orderStatus.transaction(transaction);
			    break;
			} catch (IOException e) {
			    e.printStackTrace();
			    transaction.rollback();
			    profile.retry.orderStatus++;
			}
		    }
		} else if (transactionType <= Percent.KXCT_DELIEVERY_PERCENT) {
		    delivery.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    wId = (int) delivery.warehouseId();
			    if (!doingDelivery[wId - 1].getAndSet(true)) {
				delivery.transaction(transaction);
				doingDelivery[wId - 1].set(false);
			    } else {
				pendingDelivery++;
			    }
			    break;
			} catch (IOException e) {
			    System.out.println("delivery IOException");
			    e.printStackTrace();
			    transaction.rollback();
			    profile.retry.delivery++;
			}
		    }
		} else {
		    stockLevel.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    stockLevel.transaction(transaction);
			    break;
			} catch (IOException e) {
			    e.printStackTrace();
			    transaction.rollback();
			    profile.retry.stockLevel++;
			}
		    }
		}
	    }
	    profile.elapsed = System.currentTimeMillis() - start;
	    try {
		session.close();
	    } catch (IOException e) {
		System.out.println(e);
	    }
	} catch (IOException e) {
	    System.out.println(e);
	    e.printStackTrace();
	} catch (ExecutionException e) {
	    System.out.println(e);
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    System.out.println(e);
	    e.printStackTrace();
	} catch (BrokenBarrierException e) {
            System.out.println(e);
	    e.printStackTrace();
        }
    }
}
