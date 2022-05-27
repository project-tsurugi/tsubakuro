package com.nautilus_technologies.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.tsurugidb.jogasaki.proto.SqlResponse;

public class Payment {
    SqlClient sqlClient;
    Transaction transaction;
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

    public Payment(SqlClient sqlClient, RandomGenerator randomGenerator, Profile profile) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
        this.randomGenerator = randomGenerator;
        this.warehouses = profile.warehouses;
        this.profile = profile;
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
    String sql1 = "UPDATE WAREHOUSE SET w_ytd = w_ytd + :h_amount WHERE w_id = :w_id";
    prepared1 = sqlClient.prepare(sql1,
    Placeholders.of("h_amount", double.class),
    Placeholders.of("w_id", long.class)).get();

    String sql2 = "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM WAREHOUSE WHERE w_id = :w_id";
    prepared2 = sqlClient.prepare(sql2,
    Placeholders.of("w_id", long.class)).get();

    String sql3 = "UPDATE DISTRICT SET d_ytd = d_ytd + :h_amount WHERE d_w_id = :d_w_id AND d_id = :d_id";
    prepared3 = sqlClient.prepare(sql3,
    Placeholders.of("h_amount", double.class),
    Placeholders.of("d_w_id", long.class),
    Placeholders.of("d_id", long.class)).get();

    String sql4 = "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
    prepared4 = sqlClient.prepare(sql4,
    Placeholders.of("d_w_id", long.class),
    Placeholders.of("d_id", long.class)).get();

    String sql5 = "SELECT COUNT(c_id) FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last";
    prepared5 = sqlClient.prepare(sql5,
    Placeholders.of("c_w_id", long.class),
    Placeholders.of("c_d_id", long.class),
    Placeholders.of("c_last", String.class)).get();

    String sql6 = "SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first ";
    prepared6 = sqlClient.prepare(sql6,
    Placeholders.of("c_w_id", long.class),
    Placeholders.of("c_d_id", long.class),
    Placeholders.of("c_last", String.class)).get();

    String sql7 = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
    prepared7 = sqlClient.prepare(sql7,
    Placeholders.of("c_w_id", long.class),
    Placeholders.of("c_d_id", long.class),
    Placeholders.of("c_id", long.class)).get();

    String sql8 = "SELECT c_data FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
    prepared8 = sqlClient.prepare(sql8,
    Placeholders.of("c_w_id", long.class),
    Placeholders.of("c_d_id", long.class),
    Placeholders.of("c_id", long.class)).get();

    String sql9 = "UPDATE CUSTOMER SET c_balance = :c_balance ,c_data = :c_data WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
    prepared9 = sqlClient.prepare(sql9,
    Placeholders.of("c_balance", double.class),
    Placeholders.of("c_data", String.class),
    Placeholders.of("c_w_id", long.class),
    Placeholders.of("c_d_id", long.class),
    Placeholders.of("c_id", long.class)).get();

    String sql10 = "UPDATE CUSTOMER SET c_balance = :c_balance WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id";
    prepared10 = sqlClient.prepare(sql10,
    Placeholders.of("c_balance", double.class),
    Placeholders.of("c_w_id", long.class),
    Placeholders.of("c_d_id", long.class),
    Placeholders.of("c_id", long.class)).get();
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
        if (profile.fixThreadMapping) {
            long warehouseStep = warehouses / profile.threads;
            paramsWid = randomGenerator.uniformWithin((profile.index * warehouseStep) + 1, (profile.index + 1) * warehouseStep);
        } else {
            paramsWid = randomGenerator.uniformWithin(1, warehouses);
        }
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
        paramsHamount = (randomGenerator.uniformWithin(100, 500000)) / 100.0;
        paramsByName = randomGenerator.uniformWithin(1, 100) <= 60;
        if (paramsByName) {
            paramsClast = lastName((int) randomGenerator.nonUniform255Within(0, Scale.L_NAMES - 1));  // scale::lnames
        } else {
            paramsCid = randomGenerator.nonUniform1023Within(1, Scale.CUSTOMERS);  // scale::customers
        }
        paramsHdate = dateStamp();
        paramsHdata = randomGenerator.makeAlphaString(12, 24);
        }

        void rollback() throws IOException, ServerException, InterruptedException {
        if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
            throw new IOException("error in rollback");
        }
        transaction = null;
    }

    @SuppressWarnings("checkstyle:methodlength")
    public void transaction(AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
    while (!stop.get()) {
        transaction = sqlClient.createTransaction().get();
        profile.invocation.payment++;

        // UPDATE WAREHOUSE SET w_ytd = w_ytd + :h_amount WHERE w_id = :w_id
        var future1 = transaction.executeStatement(prepared1,
        Parameters.of("h_amount", (double) paramsHamount),
        Parameters.of("w_id", (long) paramsWid));
        var result1 = future1.get();
        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(result1.getResultCase())) {
            profile.retryOnStatement.payment++;
            profile.warehouseTable.payment++;
            rollback();
            continue;
        }

        // SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM WAREHOUSE WHERE w_id = :w_id
        var future2 = transaction.executeQuery(prepared2,
        Parameters.of("w_id", (long) paramsWid));
        var resultSet2 = future2.get();
        try {
        if (!Objects.isNull(resultSet2)) {
            if (!resultSet2.nextRecord()) {
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
                    throw new IOException("SQL error");
                }
                throw new IOException("no record");
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
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
                    throw new IOException("SQL error");
                }
            throw new IOException("found multiple records");
            }
        }
        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
            throw new IOException("SQL error");
        }
        } catch (ServerException e) {
        profile.retryOnStatement.payment++;
        profile.warehouseTable.payment++;
        rollback();
        continue;
        } finally {
        if (!Objects.isNull(resultSet2)) {
            resultSet2.close();
            resultSet2 = null;
        }
        }

        // UPDATE DISTRICT SET d_ytd = d_ytd + :h_amount WHERE d_w_id = :d_w_id AND d_id = :d_id";
        var future3 = transaction.executeStatement(prepared3,
        Parameters.of("h_amount", (double) paramsHamount),
        Parameters.of("d_w_id", (long) paramsWid),
        Parameters.of("d_id", (long) paramsDid));
        var result3 = future3.get();
        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(result3.getResultCase())) {
        profile.retryOnStatement.payment++;
        profile.districtTable.payment++;
        rollback();
        continue;
        }

        // SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id
        var future4 = transaction.executeQuery(prepared4,
        Parameters.of("d_w_id", (long) paramsWid),
        Parameters.of("d_id", (long) paramsDid));
        var resultSet4 = future4.get();
        try {
        if (!Objects.isNull(resultSet4)) {
            if (!resultSet4.nextRecord()) {
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet4.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
            }
            throw new IOException("no record");
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
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet4.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
            }
            throw new IOException("found multiple records");
            }
        }
        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet4.getResponse().get().getResultCase())) {
            throw new IOException("SQL error");
        }
        } catch (ServerException e) {
        profile.retryOnStatement.payment++;
        profile.districtTable.payment++;
        rollback();
        continue;
        } finally {
        if (!Objects.isNull(resultSet4)) {
            resultSet4.close();
            resultSet4 = null;
        }
        }

        if (!paramsByName) {
        cId = paramsCid;
        } else {
        cId = Customer.chooseCustomer(transaction, prepared5, prepared6, paramsWid, paramsDid, paramsClast);
        if (cId < 0) {
            profile.retryOnStatement.payment++;
            profile.customerTable.payment++;
            rollback();
            continue;
        }
        }

        // SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
        var future7 = transaction.executeQuery(prepared7,
        Parameters.of("c_w_id", (long) paramsWid),
        Parameters.of("c_d_id", (long) paramsDid),
        Parameters.of("c_id", (long) cId));
        var resultSet7 = future7.get();
        try {
        if (!Objects.isNull(resultSet7)) {
            if (!resultSet7.nextRecord()) {
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet7.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
            }
            throw new IOException("no record");
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
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet7.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
            }
            throw new IOException("found multiple records");
            }
        }
        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet7.getResponse().get().getResultCase())) {
            throw new IOException("SQL error");
        }
        } catch (ServerException e) {
        profile.retryOnStatement.payment++;
        profile.customerTable.payment++;
        rollback();
        continue;
        } finally {
        if (!Objects.isNull(resultSet7)) {
            resultSet7.close();
            resultSet7 = null;
        }
        }

        cBalance += paramsHamount;

        if (cCredit.indexOf("BC") >= 0) {
        // SELECT c_data FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
        var future8 = transaction.executeQuery(prepared8,
        Parameters.of("c_w_id", (long) paramsWid),
        Parameters.of("c_d_id", (long) paramsDid),
        Parameters.of("c_id", (long) cId));
        var resultSet8 = future8.get();
        try {
            if (!Objects.isNull(resultSet8)) {
            if (!resultSet8.nextRecord()) {
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet8.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
                }
                throw new IOException("no record");
            }
            resultSet8.nextColumn();
            cData = resultSet8.getCharacter();
            if (resultSet8.nextRecord()) {
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet8.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
                }
                throw new IOException("found multiple records");
            }
            }
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet8.getResponse().get().getResultCase())) {
            throw new IOException("SQL error");
            }
        } catch (ServerException e) {
            profile.retryOnStatement.payment++;
            profile.customerTable.payment++;
            rollback();
            continue;
        } finally {
            if (!Objects.isNull(resultSet8)) {
            resultSet8.close();
            resultSet8 = null;
            }
        }

        String cNewData = String.format("| %4d %2d %4d %2d %4d $%7.2f ", cId, paramsDid, paramsWid, paramsDid, paramsWid, paramsHamount) + paramsHdate + " " + paramsHdata;
        int length = 500 - cNewData.length();
        if (length < cData.length()) {
            cNewData += cData.substring(0, length);
        } else {
            cNewData += cData;
        }

        // UPDATE CUSTOMER SET c_balance = :c_balance ,c_data = :c_data WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
        var future9 = transaction.executeStatement(prepared9,
        Parameters.of("c_balance", (double) cBalance),
        Parameters.of("c_data", cNewData),
        Parameters.of("c_w_id", (long) paramsWid),
        Parameters.of("c_d_id", (long) paramsDid),
        Parameters.of("c_id", (long) cId));
        var result9 = future9.get();
        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(result9.getResultCase())) {
            profile.retryOnStatement.payment++;
            profile.customerTable.payment++;
            rollback();
            continue;
        }
        } else {
        // UPDATE CUSTOMER SET c_balance = :c_balance WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
        var future10 = transaction.executeStatement(prepared10,
        Parameters.of("c_balance", (double) cBalance),
        Parameters.of("c_w_id", (long) paramsWid),
        Parameters.of("c_d_id", (long) paramsDid),
        Parameters.of("c_id", (long) cId));
        var result10 = future10.get();
        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(result10.getResultCase())) {
            profile.retryOnStatement.payment++;
            profile.customerTable.payment++;
            rollback();
            continue;
        }
        }

        var commitResponse = transaction.commit().get();
        if (SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                profile.completion.payment++;
                return;
        }
        profile.retryOnCommit.payment++;
        transaction = null;
    }
    }
}
