package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;

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
			while (true) {
			    try {
				delivery.transaction();
				doingDelivery[wId - 1].set(false);
				pendingDelivery--;
				if (pendingDelivery > 0) {
				    delivery.setParams();
				}
				break;
			    } catch (IOException e) {
				profile.retry.delivery++;
			    }
			}
			continue;
		    }
		}

		var transactionType = randomGenerator.uniformWithin(1, 100);
		if (transactionType <= Percent.KXCT_NEWORDER_PERCENT) {
		    newOrder.setParams();
		    while (true) {
			try {
			    newOrder.transaction();
			    break;
			} catch (IOException e) {
			    profile.retry.newOrder++;
			}
		    }
		} else if (transactionType <= Percent.KXCT_PAYMENT_PERCENT) {
		    payment.setParams();
		    while (true) {
			try {
			    payment.transaction();
			    break;
			} catch (IOException e) {
			    profile.retry.payment++;
			}
		    }
		} else if (transactionType <= Percent.KXCT_ORDERSTATUS_PERCENT) {
		    orderStatus.setParams();
		    while (true) {
			try {
			    orderStatus.transaction();
			    break;
			} catch (IOException e) {
			    profile.retry.orderStatus++;
			}
		    }
		} else if (transactionType <= Percent.KXCT_DELIEVERY_PERCENT) {
		    delivery.setParams();
		    while (true) {
			try {
			    wId = (int) delivery.warehouseId();
			    if (!doingDelivery[wId - 1].getAndSet(true)) {
				delivery.transaction();
				doingDelivery[wId - 1].set(false);
			    } else {
				pendingDelivery++;
			    }
			    break;
			} catch (IOException e) {
			    profile.retry.delivery++;
			}
		    }
		} else {
		    stockLevel.setParams();
		    while (true) {
			try {
			    stockLevel.transaction();
			    break;
			} catch (IOException e) {
			    profile.retry.newOrder++;
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
