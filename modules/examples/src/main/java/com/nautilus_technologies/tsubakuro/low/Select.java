package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.SessionCreator;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Select {
    Session session;
    
    public Select(Connector connector, SessionCreator sessionCreator) throws IOException, ExecutionException, InterruptedException {
	session = sessionCreator.createSession(connector).get();
    }
    
    public void select(String sql) throws IOException, ExecutionException, InterruptedException {
	Transaction transaction = session.createTransaction().get();
	ResultSet resultSet = transaction.executeQuery(sql).get();
	while (resultSet.nextRecord()) {
	    while (resultSet.nextColumn()) {
		if (!resultSet.isNull()) {
		    switch (resultSet.getRecordMeta().at()) {
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
		    case STRING:
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
}
