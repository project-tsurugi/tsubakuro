package com.nautilus_technologies.tsubakuro.low.tpch;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;
import java.util.Date;
import java.util.Locale;
import java.text.SimpleDateFormat;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Q14 {
    Session session;
    PreparedStatement preparedT;
    PreparedStatement preparedB;

    public Q14(Session session) throws IOException, ExecutionException, InterruptedException {
        this.session = session;
	prepare();
    }

    public void prepare() throws IOException, ExecutionException, InterruptedException {
	String sqlT = "SELECT "
	    + "SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS MOLECULE "
	    + "FROM LINEITEM, PART "
	    + "WHERE "
	    + "L_PARTKEY = P_PARTKEY "
	    + "AND P_TYPE1 = 'PROMO' "
	    + "AND L_SHIPDATE >= :datefrom "
	    + "AND L_SHIPDATE < :dateto";

	var ph = RequestProtos.PlaceHolder.newBuilder()
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("dateto").setType(CommonProtos.DataType.CHARACTER))
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("datefrom").setType(CommonProtos.DataType.CHARACTER))
	    .build();
	preparedT = session.prepare(sqlT, ph).get();

	String sqlB = "SELECT "
	    + "SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS DENOMINATOR "
	    + "FROM LINEITEM, PART "
	    + "WHERE "
	    + "L_PARTKEY = P_PARTKEY "
	    + "AND L_SHIPDATE >= :datefrom "
	    + "AND L_SHIPDATE < :dateto";

	preparedB = session.prepare(sqlB, ph).get();
    }

    public void run(Profile profile) throws IOException, ExecutionException, InterruptedException {
	long start = System.currentTimeMillis();
	var transaction = session.createTransaction(profile.transactionOption.build()).get();

	var ps = RequestProtos.ParameterSet.newBuilder();
	if (profile.queryValidation) {
	    ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("datefrom").setCharacterValue("1995-09-01")).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("dateto").setCharacterValue("1995-10-01"));
        } else {
	    ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("datefrom").setCharacterValue("1997-11-01")).
		addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("dateto").setCharacterValue("1997-12-01"));
        }

	long t, b;

	var futureT = transaction.executeQuery(preparedT, ps.build());
	var resultSetT = futureT.get();
	try {
	    if (Objects.nonNull(resultSetT)) {
		if (resultSetT.nextRecord()) {
		    resultSetT.nextColumn();
		    if (!resultSetT.isNull()) {
			t = resultSetT.getInt8();
		    } else {
			System.out.println("REVENUE is null");
			throw new ExecutionException(new IOException("column is null"));
		    }
		} else {
		    throw new ExecutionException(new IOException("no record"));
		}
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSetT.getFutureResponse().get().getResultCase())) {
		    throw new ExecutionException(new IOException("SQL error"));
		}
	    } else {
		throw new ExecutionException(new IOException("no resultSet"));
	    }
	} catch (ExecutionException e) {
	    throw new IOException(e);
	} finally {
	    if (!Objects.isNull(resultSetT)) {
		resultSetT.close();
	    }
	}

	var futureB = transaction.executeQuery(preparedB, ps.build());
	var resultSetB = futureB.get();
	try {
	    if (Objects.nonNull(resultSetB)) {
		if (resultSetB.nextRecord()) {
		    resultSetB.nextColumn();
		    if (!resultSetB.isNull()) {
			b = resultSetB.getInt8();
			System.out.println("PROMO_REVENUE");
			System.out.println(t + " / " + b + " = " + (100.0 * (double) t) / (double) b + " (%)");
		    } else {
			System.out.println("REVENUE is null");
		    }
		} else {
		    throw new ExecutionException(new IOException("no record"));
		}
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSetB.getFutureResponse().get().getResultCase())) {
		    throw new ExecutionException(new IOException("SQL error"));
		}
	    } else {
		throw new ExecutionException(new IOException("no resultSet"));
	    }
	} catch (ExecutionException e) {
	    throw new IOException(e);
	} finally {
	    if (!Objects.isNull(resultSetB)) {
		resultSetB.close();
	    }
	}

	try {
	    var commitResponse = transaction.commit().get();
	    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(commitResponse.getResultCase())) {
		throw new IOException("commit error");
	    }
	} catch (ExecutionException e) {
	    throw new IOException(e);
	}
	profile.q14 = System.currentTimeMillis() - start;
    }
}
