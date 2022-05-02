package com.nautilus_technologies.tsubakuro.low.warehouse;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;

public class Select {
    Session session;
    
    public Select(Session session) throws IOException, ServerException, InterruptedException {
        this.session = session;
    }
    
    void printResultset(ResultSet resultSet) throws InterruptedException, IOException {
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

    public void select() throws IOException, ServerException, InterruptedException {
	String sql = "SELECT * FROM WAREHOUSE";

	Transaction transaction = session.createTransaction().get();
	var resultSet = transaction.executeQuery(sql).get();
	printResultset(resultSet);
	resultSet.getResponse().get();
	resultSet.close();
	transaction.commit().get();
	session.close();
    }
}
