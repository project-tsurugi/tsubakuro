package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Select extends Thread {
    Session session;
    PreparedStatement preparedStatement;
    Transaction transaction;
    long index;
    long pendings;
    long loop;

    int columnInt;
    long columnLong;
    float columnFloat;
    double columnDouble;
    String columnString;
    
    public Select(Connector connector, Session session, long index, long pendings, long loop) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
	this.index = index;
	this.pendings = pendings;
	this.loop = loop;
    }
    
    void printResultset(ResultSet resultSet) throws IOException {
	while (resultSet.nextRecord()) {
	    while (resultSet.nextColumn()) {
		if (!resultSet.isNull()) {
		    switch (resultSet.getRecordMeta().at()) {
		    case INT4:
			columnInt = resultSet.getInt4();
			break;
		    case INT8:
			columnLong = resultSet.getInt8();
			break;
		    case FLOAT4:
			columnFloat = resultSet.getFloat4();
			break;
		    case FLOAT8:
			columnDouble = resultSet.getFloat8();
			break;
		    case CHARACTER:
			columnString = resultSet.getCharacter();
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

    public void prepare() throws IOException, ExecutionException, InterruptedException {
	String sql = "SELECT * FROM ORDERS WHERE o_id = :o_id";
	var ph = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8));
	preparedStatement = session.prepare(sql, ph).get();
    }
    public void run() {
	try {
	    prepare();

            Queue<Future<ResultSet>> queue = new ArrayDeque<>();

	    transaction = session.createTransaction().get();
            for (long i = 0; i < loop; i++) {
		var ps = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(index + 1));
		queue.add(transaction.executeQuery(preparedStatement, ps));
		if (queue.size() > pendings) {
		    var resultSet = queue.poll().get();
		    printResultset(resultSet);
		    resultSet.close();
		}
	    }
            while (queue.size() > 0) {
		var resultSet = queue.poll().get();
		printResultset(resultSet);
		resultSet.close();
            }
	    transaction.commit().get();

	    preparedStatement.close();
	    session.close();
        } catch (IOException e) {
            System.out.println(e);
        } catch (ExecutionException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
    public void dummy() {
	System.out.println(columnInt);
	System.out.println(columnLong);
	System.out.println(columnFloat);
	System.out.println(columnDouble);
	System.out.println(columnString);
    }
}
