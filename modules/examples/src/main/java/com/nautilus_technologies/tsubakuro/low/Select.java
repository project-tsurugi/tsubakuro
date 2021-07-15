package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Select {
    Session session;
    Transaction transaction;
    PreparedStatement preparedStatement;
    
    public Select(Connector connector, Session session) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
    }
    
    void printResultset(ResultSet resultSet) throws IOException {
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

    public void select(String sql) throws IOException, ExecutionException, InterruptedException {
	transaction = session.createTransaction().get();
	printResultset(transaction.executeQuery(sql).get());
	transaction.commit().get();
    }

    public void select() throws IOException, ExecutionException, InterruptedException {
	select("SELECT * FROM ORDERS WHERE o_id = 99999999");
    }

    public void prepareAndSelect() throws IOException, ExecutionException, InterruptedException {
	String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
	var ph = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8));
	preparedStatement = session.prepare(sql, ph).get();

	transaction = session.createTransaction().get();
	var ps = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setLValue(99999999));
	var resultSet = transaction.executeQuery(preparedStatement, ps).get();
	printResultset(resultSet);
	preparedStatement.close();
	resultSet.close();
	transaction.commit().get();
    }

    public void run(long n) throws IOException, ExecutionException, InterruptedException {
	String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
	var ph = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8));
	preparedStatement = session.prepare(sql, ph).get();

	transaction = session.createTransaction().get();
	var ps = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setLValue(99999999));

	long i;
	for (i = 0; i < n; i++) {
	    var resultSet = transaction.executeQuery(preparedStatement, ps).get();
	    while (resultSet.nextRecord()) {
		while (resultSet.nextColumn()) {
		    if (!resultSet.isNull()) {
			switch (resultSet.getRecordMeta().at()) {
			case INT4:
			    resultSet.getInt4();
			    break;
			case INT8:
			    resultSet.getInt8();
			    break;
			case FLOAT4:
			    resultSet.getFloat4();
			    break;
			case FLOAT8:
			    resultSet.getFloat8();
			    break;
			case STRING:
			    resultSet.getCharacter();
			    break;
			default:
			    throw new IOException("the column type is invalid");
			}
		    }
		}
	    }
	    resultSet.close();
	    if ((i % 100000) == 99999) {
		System.out.println(i);
	    }
	}
	transaction.commit().get();
	preparedStatement.close();
	System.out.println(i);
    }
}
