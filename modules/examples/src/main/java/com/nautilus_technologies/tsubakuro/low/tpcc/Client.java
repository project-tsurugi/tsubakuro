package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

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
				if (delivery.transaction(transaction)) {
				    doingDelivery[wId - 1].set(false);
				    pendingDelivery--;
				    if (pendingDelivery > 0) {
					delivery.setParams();
				    }
				    break;
				} else {
				    doingDelivery[wId - 1].set(false);
				}
			    } catch (IOException e) {
				doingDelivery[wId - 1].set(false);
				profile.retryOnStatement.delivery++;
				if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(transaction.rollback().get().getResultCase())) {
				    e.printStackTrace();
				    throw new IOException("error in rollback");
				}
			    }
			}
			continue;  // next transaction
		    }
		}

		var transactionType = randomGenerator.uniformWithin(1, 100);
		if (transactionType <= Percent.KXCT_NEWORDER_PERCENT) {
		    newOrder.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    if (newOrder.transaction(transaction)) {
				break;
			    }
			} catch (IOException e) {
			    profile.retryOnStatement.newOrder++;
			    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(transaction.rollback().get().getResultCase())) {
				e.printStackTrace();
				throw new IOException("error in rollback");
			    }
			}
		    }
		} else if (transactionType <= Percent.KXCT_PAYMENT_PERCENT) {
		    payment.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    if (payment.transaction(transaction)) {
				break;
			    }
			} catch (IOException e) {
			    profile.retryOnStatement.payment++;
			    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(transaction.rollback().get().getResultCase())) {
				e.printStackTrace();
				throw new IOException("error in rollback");
			    }
			}
		    }
		} else if (transactionType <= Percent.KXCT_ORDERSTATUS_PERCENT) {
		    orderStatus.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    if (orderStatus.transaction(transaction)) {
				break;
			    }
			} catch (IOException e) {
			    profile.retryOnStatement.orderStatus++;
			    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(transaction.rollback().get().getResultCase())) {
				e.printStackTrace();
				throw new IOException("error in rollback");
			    }
			}
		    }
		} else if (transactionType <= Percent.KXCT_DELIEVERY_PERCENT) {
		    delivery.setParams();
		    wId = (int) delivery.warehouseId();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    if (!doingDelivery[wId - 1].getAndSet(true)) {
				if (delivery.transaction(transaction)) {
				    doingDelivery[wId - 1].set(false);
				    break;
				} else {
				    doingDelivery[wId - 1].set(false);
				}
			    } else {
				pendingDelivery++;
				break;
			    }
			} catch (IOException e) {
			    doingDelivery[wId - 1].set(false);
			    profile.retryOnStatement.delivery++;
			    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(transaction.rollback().get().getResultCase())) {
				e.printStackTrace();
				throw new IOException("error in rollback");
			    }
			}
		    }
		} else {
		    stockLevel.setParams();
		    while (!stop.get()) {
			var transaction = createTransaction();
			try {
			    if (stockLevel.transaction(transaction)) {
				break;
			    }
			} catch (IOException e) {
			    profile.retryOnStatement.stockLevel++;
			    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(transaction.rollback().get().getResultCase())) {
				e.printStackTrace();
				throw new IOException("error in rollback");
			    }
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
	} catch (IOException | ExecutionException | InterruptedException | BrokenBarrierException e) {
	    System.out.println(e);
	    e.printStackTrace();
        }
    }
}
