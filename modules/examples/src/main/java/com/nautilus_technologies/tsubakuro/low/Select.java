package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Select {
    Session session;
    PreparedStatement preparedStatement;
    
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

    public void prepareAndSelect() throws IOException, ExecutionException, InterruptedException {
	String sql = "SELECT * FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id";
	var ph = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
	    .build();
	preparedStatement = session.prepare(sql, ph).get();

	try {
	    Transaction transaction = session.createTransaction().get();
	    var ps = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(99999999))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(3))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(1))
		.build();
	    var pair = transaction.executeQuery(preparedStatement, ps);
	    var resultSet = pair.getLeft().get();
	    if (!Objects.isNull(resultSet)) {
		printResultset(resultSet);
		resultSet.close();
	    }
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(pair.getRight().get().getResultCase())) {
		throw new IOException("select error");
	    }
	    var commitResponse = transaction.commit().get();
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
		throw new IOException("commit (select) error");
	    }
	} catch (IOException e) {
	    throw e;
	} finally {
	    preparedStatement.close();
	    session.close();
	}
    }
}
