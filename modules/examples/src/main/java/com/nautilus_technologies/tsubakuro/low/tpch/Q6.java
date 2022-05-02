package com.nautilus_technologies.tsubakuro.low.tpch;

import java.io.IOException;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

public class Q6 {
    Session session;
    PreparedStatement prepared;

    public Q6(Session session) throws IOException, ServerException, InterruptedException {
        this.session = session;
	prepare();
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
	String sql = "SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE "
	    + "FROM LINEITEM "
	    + "WHERE "
	    + "L_SHIPDATE >= :datefrom "
	    + "AND L_SHIPDATE < :dateto "
	    // "AND L_DISCOUNT BETWEEN 0.03 - 0.01 and 0.03 + 0.01"
	    + "AND L_DISCOUNT >= :discount - 1 "
	    + "AND L_DISCOUNT <= :discount + 1 "
	    + "AND L_QUANTITY < :quantity";

	var ph = RequestProtos.PlaceHolder.newBuilder()
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("dateto").setType(CommonProtos.DataType.CHARACTER))
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("datefrom").setType(CommonProtos.DataType.CHARACTER))
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("discount").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("quantity").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared = session.prepare(sql, ph).get();
    }

    public void run(Profile profile) throws IOException, ServerException, InterruptedException {
	long start = System.currentTimeMillis();
	var transaction = session.createTransaction(profile.transactionOption.build()).get();

	var ps = RequestProtos.ParameterSet.newBuilder();
	if (profile.queryValidation) {
	    ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("datefrom").setCharacterValue("1994-01-01")).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("dateto").setCharacterValue("1995-01-01")).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("discount").setInt8Value(6)).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("quantity").setInt8Value(24));
        } else {
	    ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("datefrom").setCharacterValue("1995-01-01")).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("dateto").setCharacterValue("1996-01-01")).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("discount").setInt8Value(9)).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("quantity").setInt8Value(25));
        }

	var future = transaction.executeQuery(prepared, ps.build());
	var resultSet = future.get();

	try {
	    if (Objects.nonNull(resultSet)) {
		if (resultSet.nextRecord()) {
		    resultSet.nextColumn();
		    if (!resultSet.isNull()) {
			System.out.println("REVENUE");
			System.out.println(resultSet.getInt8());
		    } else {
			System.out.println("REVENUE is null");
		    }
		} else {
		    throw new IOException("no record");
		}
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet.getResponse().get().getResultCase())) {
		    throw new IOException("SQL error");
		}
	    } else {
		throw new IOException("no resultSet");
	    }

	    var commitResponse = transaction.commit().get();
	    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(commitResponse.getResultCase())) {
		throw new IOException("commit error");
	    }
	} catch (ServerException e) {
	    throw new IOException(e);
	} finally {
	    if (!Objects.isNull(resultSet)) {
		resultSet.close();
	    }
	}
	profile.q6 = System.currentTimeMillis() - start;
    }
}
