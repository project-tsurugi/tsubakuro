package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Date;
import java.util.Locale;
import java.text.SimpleDateFormat;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Payment {
    Session session;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared1;
    PreparedStatement prepared2;
    PreparedStatement prepared3;
    PreparedStatement prepared4;
    PreparedStatement prepared5;
    PreparedStatement prepared6;
    PreparedStatement prepared7;
    PreparedStatement prepared8;
    PreparedStatement prepared9;
    PreparedStatement prepared10;

    long warehouses;
    long paramsWid;
    long paramsDid;
    long paramsCid;
    double paramsHamount;
    boolean paramsByName;
    String paramsClast;
    String paramsHdate;
    String paramsHdata;

    //  local variables
    String wName;
    String wStreet1;
    String wStreet2;
    String wCity;
    String wState;
    String wZip;
    String dName;
    String dStreet1;
    String dStreet2;
    String dCity;
    String dState;
    String dZip;
    long cId;
    String cFirst;
    String cMiddle;
    String cLast;
    String cStreet1;
    String cStreet2;
    String cCity;
    String cState;
    String cZip;
    String cPhone;
    String cCredit;
    double cCreditLim;

    double cDiscount;
    double cBalance;
    String cSince;
    String cData;

    static String[] nameParts = {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
				 "ESE", "ANTI", "CALLY", "ATION", "EING"};

    public Payment(Session session, RandomGenerator randomGenerator, Profile profile) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.randomGenerator = randomGenerator;
	this.warehouses = profile.warehouses;
	this.profile = profile;
    }

    public void prepare() throws IOException, ExecutionException, InterruptedException {
	String sql1 = "UPDATE WAREHOUSE SET w_ytd = w_ytd + :h_amount WHERE w_id = :w_id";
	var ph1 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("h_amount").setType(CommonProtos.DataType.FLOAT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("w_id").setType(CommonProtos.DataType.INT8));
	prepared1 = session.prepare(sql1, ph1).get();

	String sql2 = "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM WAREHOUSE WHERE w_id = :w_id";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("w_id").setType(CommonProtos.DataType.INT8));
	prepared2 = session.prepare(sql2, ph2).get();

	String sql3 = "UPDATE DISTRICT SET d_ytd = d_ytd + :h_amount WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ph3 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("h_amount").setType(CommonProtos.DataType.FLOAT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_id").setType(CommonProtos.DataType.INT8));
	prepared3 = session.prepare(sql3, ph3).get();

	String sql4 = "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ph4 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_id").setType(CommonProtos.DataType.INT8));
	prepared4 = session.prepare(sql4, ph4).get();

	String sql5 = "SELECT COUNT(c_id) FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last";
	var ph5 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_last").setType(CommonProtos.DataType.CHARACTER));
	prepared5 = session.prepare(sql5, ph5).get();

	String sql6 = "SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first ";
	var ph6 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_last").setType(CommonProtos.DataType.CHARACTER));
	prepared6 = session.prepare(sql6, ph6).get();

	String sql7 = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
	var ph7 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8));
	prepared7 = session.prepare(sql7, ph7).get();

	String sql8 = "SELECT c_data FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
	var ph8 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8));
	prepared8 = session.prepare(sql8, ph8).get();

	String sql9 = "UPDATE CUSTOMER SET c_balance = :c_balance ,c_data = :c_data WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
	var ph9 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_balance").setType(CommonProtos.DataType.FLOAT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_data").setType(CommonProtos.DataType.CHARACTER))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8));
	prepared9 = session.prepare(sql9, ph9).get();

	String sql10 = "UPDATE CUSTOMER SET c_balance = :c_balance WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
	var ph10 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_balance").setType(CommonProtos.DataType.FLOAT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8));
	prepared10 = session.prepare(sql10, ph10).get();
    }

    static String dateStamp() {
	Date date = new Date();
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", new Locale("US"));
	return dateFormat.format(date);
    }

    static String lastName(int num) {
	String name = nameParts[num / 100];
	name += nameParts[(num / 10) % 10];
	name += nameParts[num % 10];
	return name;
    }

    public void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, warehouses);  // FIXME warehouse_low, warehouse_high
	paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
	paramsHamount = ((double) randomGenerator.uniformWithin(100, 500000)) / 100.0;
	paramsByName = randomGenerator.uniformWithin(1, 100) <= 60;
	if (paramsByName) {
	    paramsClast = lastName((int) randomGenerator.nonUniform255Within(0, Scale.L_NAMES - 1));  // scale::lnames
	} else {
	    paramsCid = randomGenerator.nonUniform1023Within(1, Scale.CUSTOMERS);  // scale::customers
	}
	paramsHdate = dateStamp();
	paramsHdata = randomGenerator.makeAlphaString(12, 24);
    }

    void rollback(Transaction transaction) throws IOException, ExecutionException, InterruptedException {
	if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
	    throw new IOException("error in rollback");
	}
    }

    public void transaction(AtomicBoolean stop) throws IOException, ExecutionException, InterruptedException {
	while (!stop.get()) {
	    var transaction = session.createTransaction().get();
	    profile.invocation.payment++;
	    //  transaction logic
	    if (!firstHalf(transaction)) {
		continue;
	    }
	    if (cId != 0) {
		if (!secondHalf(transaction)) {
		    continue;
		}
	    }

	    var commitResponse = transaction.commit().get();
	    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
		profile.completion.payment++;
		return;
	    }
	    profile.retryOnCommit.payment++;
	}
    }

    boolean firstHalf(Transaction transaction) throws IOException, ExecutionException, InterruptedException {
	// UPDATE WAREHOUSE SET w_ytd = w_ytd + :h_amount WHERE w_id = :w_id
	var ps1 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("h_amount").setFloat8Value(paramsHamount))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("w_id").setInt8Value(paramsWid));
	var future1 = transaction.executeStatement(prepared1, ps1);
	var result1 = future1.get();
	if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result1.getResultCase())) {
	    profile.retryOnStatement.payment++;
	    rollback(transaction);
	    return false;
	}

	// SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM WAREHOUSE WHERE w_id = :w_id
	var ps2 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("w_id").setInt8Value(paramsWid));
	var future2 = transaction.executeQuery(prepared2, ps2);
	try {
	    var resultSet2 = future2.get();
	    if (!resultSet2.nextRecord()) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	    resultSet2.nextColumn();
	    wName = resultSet2.getCharacter();
	    resultSet2.nextColumn();
	    wStreet1 = resultSet2.getCharacter();
	    resultSet2.nextColumn();
	    wStreet2 = resultSet2.getCharacter();
	    resultSet2.nextColumn();
	    wCity = resultSet2.getCharacter();
	    resultSet2.nextColumn();
	    wState = resultSet2.getCharacter();
	    resultSet2.nextColumn();
	    wZip = resultSet2.getCharacter();
	    if (resultSet2.nextRecord()) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	    resultSet2.close();
	} catch (ExecutionException e) {
	    profile.retryOnStatement.payment++;
	    rollback(transaction);
	    return false;
	}

	// UPDATE DISTRICT SET d_ytd = d_ytd + :h_amount WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ps3 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("h_amount").setFloat8Value(paramsHamount))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_id").setInt8Value(paramsDid));
	var future3 = transaction.executeStatement(prepared3, ps3);
	var result3 = future3.get();
	if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result3.getResultCase())) {
	    profile.retryOnStatement.payment++;
	    rollback(transaction);
	    return false;
	}

	// SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id
	var ps4 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_id").setInt8Value(paramsDid));
	var future4 = transaction.executeQuery(prepared4, ps4);
	try {
	    var resultSet4 = future4.get();
	    if (!resultSet4.nextRecord()) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	    resultSet4.nextColumn();
	    dStreet1 = resultSet4.getCharacter();
	    resultSet4.nextColumn();
	    dStreet2 = resultSet4.getCharacter();
	    resultSet4.nextColumn();
	    dCity = resultSet4.getCharacter();
	    resultSet4.nextColumn();
	    dState = resultSet4.getCharacter();
	    resultSet4.nextColumn();
	    dZip = resultSet4.getCharacter();
	    resultSet4.nextColumn();
	    dName = resultSet4.getCharacter();
	    if (resultSet4.nextRecord()) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	    resultSet4.close();
	} catch (ExecutionException e) {
	    profile.retryOnStatement.payment++;
	    rollback(transaction);
	    return false;
	}

	if (!paramsByName) {
	    cId = paramsCid;
	} else {
	    cId = Customer.chooseCustomer(transaction, prepared5, prepared6, paramsWid, paramsDid, paramsClast);
	}
	return true;
    }

    boolean secondHalf(Transaction transaction) throws IOException, ExecutionException, InterruptedException {
	// SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
	var ps7 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(cId));
	var future7 = transaction.executeQuery(prepared7, ps7);
	try {
	    var resultSet7 = future7.get();
	    if (!resultSet7.nextRecord()) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	    resultSet7.nextColumn();
	    cFirst = resultSet7.getCharacter();  // c_first(0)
	    resultSet7.nextColumn();
	    cMiddle = resultSet7.getCharacter();  // c_middle(1)
	    resultSet7.nextColumn();
	    cLast = resultSet7.getCharacter();  // c_last(2)
	    resultSet7.nextColumn();
	    cStreet1 = resultSet7.getCharacter();  // c_street_1(3)
	    resultSet7.nextColumn();
	    cStreet2 = resultSet7.getCharacter();  // c_street_1(4)
	    resultSet7.nextColumn();
	    cCity = resultSet7.getCharacter();  // c_city(5)
	    resultSet7.nextColumn();
	    cState = resultSet7.getCharacter();  // c_state(6)
	    resultSet7.nextColumn();
	    cZip = resultSet7.getCharacter();  // c_zip(7)
	    resultSet7.nextColumn();
	    cPhone = resultSet7.getCharacter();  // c_phone(8)
	    resultSet7.nextColumn();
	    cCredit = resultSet7.getCharacter();  // c_credit(9)
	    resultSet7.nextColumn();
	    cCreditLim = resultSet7.getFloat8();  // c_credit_lim(10)
	    resultSet7.nextColumn();
	    cDiscount = resultSet7.getFloat8();  // c_discount(11)
	    resultSet7.nextColumn();
	    cBalance = resultSet7.getFloat8();  // c_balance(12)
	    resultSet7.nextColumn();
	    cSince = resultSet7.getCharacter();  // c_since(13)
	    if (resultSet7.nextRecord()) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	    resultSet7.close();
	} catch (ExecutionException e) {
	    profile.retryOnStatement.payment++;
	    rollback(transaction);
	    return false;
	}

	cBalance += paramsHamount;

	if (cCredit.indexOf("BC") >= 0) {
	    // SELECT c_data FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
	    var ps8 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(cId));
	    var future8 = transaction.executeQuery(prepared8, ps8);
	    try {
		var resultSet8 = future8.get();
		if (!resultSet8.nextRecord()) {
		    profile.retryOnStatement.payment++;
		    rollback(transaction);
		    return false;
		}
		resultSet8.nextColumn();
		cData = resultSet8.getCharacter();
		if (resultSet8.nextRecord()) {
		    profile.retryOnStatement.payment++;
		    rollback(transaction);
		    return false;
		}
		resultSet8.close();
	    } catch (ExecutionException e) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }

	    String cNewData = String.format("| %4d %2d %4d %2d %4d $%7.2f ", cId, paramsDid, paramsWid, paramsDid, paramsWid, paramsHamount) + paramsHdate + " " + paramsHdata;
	    int length = 500 - cNewData.length();
	    if (length < cData.length()) {
		cNewData += cData.substring(0, length);
	    } else {
		cNewData += cData;
	    }

	    // UPDATE CUSTOMER SET c_balance = :c_balance ,c_data = :c_data WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
	    var ps9 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_balance").setFloat8Value(cBalance))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_data").setCharacterValue(cNewData))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(cId));
	    var future9 = transaction.executeStatement(prepared9, ps9);
	    var result9 = future9.get();
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result9.getResultCase())) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	} else {
	    // UPDATE CUSTOMER SET c_balance = :c_balance WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
	    var ps10 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_balance").setFloat8Value(cBalance))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(cId));
	    var future10 = transaction.executeStatement(prepared10, ps10);
	    var result10 = future10.get();
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result10.getResultCase())) {
                profile.retryOnStatement.payment++;
                rollback(transaction);
                return false;
	    }
	}
	return true;
    }
}
