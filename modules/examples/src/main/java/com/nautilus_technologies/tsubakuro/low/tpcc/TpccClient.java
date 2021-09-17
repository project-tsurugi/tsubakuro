package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;

public class  TpccClient extends Thread {
    Session session;
    RandomGenerator randomGenerator;
    long warehouses;
    NewOrder newOrder;
    Payment payment;
    Delivery delivery;
    OrderStatus orderStatus;
    StockLevel stockLevel;
    
    public  TpccClient(Connector connector, Session session) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
	this.randomGenerator = new RandomGenerator();
	this.warehouses = warehouses();
	
	this.newOrder = new NewOrder(session, randomGenerator, warehouses);
	this.payment = new Payment(session, randomGenerator, warehouses);
	this.delivery = new Delivery(session, randomGenerator, warehouses);
	this.orderStatus = new OrderStatus(session, randomGenerator, warehouses);
	this.stockLevel = new StockLevel(session, randomGenerator, warehouses);
	prepare();
    }

    long warehouses()  throws IOException, ExecutionException, InterruptedException {
	var transaction = session.createTransaction().get();
	var resultSet = transaction.executeQuery("SELECT COUNT(w_id) FROM WAREHOUSE").get();
	long count = 0;
	if (resultSet.nextRecord()) {
	    if (resultSet.nextColumn()) {
		count = resultSet.getInt8();
	    }
	}
	transaction.commit().get();
	return count;
    }

    void prepare()  throws IOException, ExecutionException, InterruptedException {
	newOrder.prepare();
	payment.prepare();
	delivery.prepare();
	orderStatus.prepare();
	stockLevel.prepare();
    }

    public void run() {
	try {
	    //	    newOrder.transaction();
	    //	    payment.transaction();
	    //	    delivery.transaction();
	    //	    orderStatus.transaction();
	    //	    stockLevel.transaction();

	    session.close();
	} catch (IOException e) {
            System.out.println(e);
	    //	} catch (ExecutionException e) {
	    //	    System.out.println(e);
	    //	} catch (InterruptedException e) {
	    //	    System.out.println(e);
        }
    }
}
