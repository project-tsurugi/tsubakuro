package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public final class Customer {
    private Customer() {
    }

    public static long chooseCustomer(Transaction transaction,
			       PreparedStatement prepared1, PreparedStatement prepared2,
			       long paramsWid, long paramsDid, String paramsClast)
	throws IOException, ExecutionException, InterruptedException {

	// SELECT COUNT(c_id) FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last"
	var ps1 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_last").setCharacterValue(paramsClast));
	var future1 = transaction.executeQuery(prepared1, ps1);
	long nameCnt = 0;
	try {
	    var resultSet1 = future1.getLeft().get();
	    if (!Objects.isNull(resultSet1)) {
		if (!resultSet1.nextRecord()) {
		    future1.getRight().get();
		    throw new ExecutionException(new IOException("no record"));
		}
		resultSet1.nextColumn();
		nameCnt = resultSet1.getInt8();
		if (resultSet1.nextRecord()) {
		    future1.getRight().get();
		    throw new ExecutionException(new IOException("found multiple records"));
		}
		resultSet1.close();
	    }
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future1.getRight().get().getResultCase())) {
		throw new ExecutionException(new IOException("SQL error"));
	    }
	} catch (ExecutionException e) {
	    return -1;
	}

	if (nameCnt == 0) {
	    return 0;
	}

	// SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first"
	var ps2 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_last").setCharacterValue(paramsClast));
	var future2 = transaction.executeQuery(prepared2, ps2);
	long rv = -1;
	try {
	    var resultSet2 = future2.getLeft().get();
	    if (!Objects.isNull(resultSet2)) {
		if ((nameCnt % 2) > 0) {
		    nameCnt++;
		}
		for (long i = 0; i < (nameCnt / 2); i++) {
		    if (!resultSet2.nextRecord()) {
			future2.getRight().get();
			throw new ExecutionException(new IOException("no record"));
		    }
		}
		resultSet2.nextColumn();
		rv = resultSet2.getInt8();
		resultSet2.close();
	    }
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future2.getRight().get().getResultCase())) {
		throw new ExecutionException(new IOException("SQL error"));
	    }
	    return rv;
	} catch (ExecutionException e) {
	    return -1;
	}
    }
}
