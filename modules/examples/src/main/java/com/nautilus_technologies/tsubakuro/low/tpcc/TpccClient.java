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
    
    public  TpccClient(Connector connector, Session session) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
	this.randomGenerator = new RandomGenerator();
	this.warehouses = warehouses();
	
	this.newOrder = new NewOrder(session, randomGenerator, warehouses);
	prepare();
    }

    long warehouses()  throws IOException, ExecutionException, InterruptedException {
	var transaction = session.createTransaction().get();
	var resultSet = transaction.executeQuery("SELECT COUNT(*) FROM WAREHOUSE").get();
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
    }

    public void run() {
	try {
	    newOrder.transaction();

	    session.close();
	} catch (IOException e) {
            System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}
