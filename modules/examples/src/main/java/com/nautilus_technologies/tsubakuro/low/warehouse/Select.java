package com.nautilus_technologies.tsubakuro.low.warehouse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Select {
    Session session;
    
    public Select(Connector connector, Session session) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
    }
    
    void printResultset(ResultSet resultSet) throws IOException {
	int count = 1;

	while (resultSet.nextRecord()) {
	    System.out.println("---- ( " + count + " )----");
	    count++;
	    while (resultSet.nextColumn()) {
		if (!resultSet.isNull()) {
		    switch (resultSet.type()) {
		    case INT4:
			System.out.println(resultSet.getInt4());
			break;
		    case INT8:
			System.out.println(resultSet.getInt8());
			break;
		    case FLOAT4:
			System.out.println(resultSet.getFloat4());
			break;
		    case FLOAT8:
			System.out.println(resultSet.getFloat8());
			break;
		    case CHARACTER:
			System.out.println(resultSet.getCharacter());
			break;
		    default:
			throw new IOException("the column type is invalid");
		    }
		} else {
		    System.out.println("the column is NULL");
		}
	    }
	}
    }

    public void select() throws IOException, ExecutionException, InterruptedException {
	String sql = "SELECT * FROM WAREHOUSE";

	Transaction transaction = session.createTransaction().get();
	var pair = transaction.executeQuery(sql);
	var resultSet = pair.getLeft().get();
	printResultset(resultSet);
	pair.getRight().get();
	resultSet.close();
	transaction.commit().get();
	session.close();
    }
}
