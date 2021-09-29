package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
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
	long nameCnt;
	try {
	    var resultSet1 = future1.get();
	    if (!resultSet1.nextRecord()) {
		throw new IOException("no record");
	    }
	    resultSet1.nextColumn();
	    nameCnt = resultSet1.getInt8();
	    if (resultSet1.nextRecord()) {
		throw new IOException("extra record");
	    }
	    resultSet1.close();
	} catch (ExecutionException e) {
		throw new IOException(e);
	}

	// SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first"
	var ps2 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_last").setCharacterValue(paramsClast));
	var future2 = transaction.executeQuery(prepared2, ps2);
	try {
	    var resultSet2 = future2.get();
	    if (nameCnt == 0) {
		return 0;
	    }
	    if ((nameCnt % 2) > 0) {
		nameCnt++;
	    }
	    for (long i = 0; i < (nameCnt / 2); i++) {
		resultSet2.nextRecord();
	    }
	    resultSet2.nextColumn();
	    var rv = resultSet2.getInt8();
	    while (resultSet2.nextRecord()) {
		resultSet2.nextColumn();
	    }
	    resultSet2.close();
	    return rv;
	} catch (ExecutionException e) {
	    throw new IOException(e);
	}
    }
}
