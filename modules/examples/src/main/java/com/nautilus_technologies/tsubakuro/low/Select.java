package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;

import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionCreatorImpl;

public final class Select {
    private Select() {
    }
    
    private static String dbName = "tsubakuro";
    
    public static void main(String[] args) {
	try {
	    Connector connector = new IpcConnectorImpl(dbName);

	    Session session = SessionCreatorImpl.createSession(connector).get();
	    Transaction transaction = session.createTransaction().get();
	    ResultSet resultSet = transaction.executeQuery(args[0]).get();
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
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}
