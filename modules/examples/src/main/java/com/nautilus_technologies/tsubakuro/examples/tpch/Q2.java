package com.nautilus_technologies.tsubakuro.low.tpch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

public class Q2 {
    Session session;
    PreparedStatement prepared1;
    PreparedStatement prepared2;
    static final int PARTKEY_SIZE = 200000;
    Map<Integer, Long> q2intermediate;

    public Q2(Session session) throws IOException, ServerException, InterruptedException {
        this.session = session;
        this.q2intermediate = new HashMap<>();
        prepare();
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
        String sql1 = "SELECT MIN(PS_SUPPLYCOST) "
                + "FROM PARTSUPP, SUPPLIER, NATION, REGION "
                + "WHERE "
                + "PS_SUPPKEY = S_SUPPKEY "
                + "AND S_NATIONKEY = N_NATIONKEY "
                + "AND N_REGIONKEY = R_REGIONKEY "
                + "AND R_NAME = :region "
                + "AND PS_PARTKEY = :partkey ";
        var ph1 = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("region").setType(CommonProtos.DataType.CHARACTER))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("partkey").setType(CommonProtos.DataType.INT8))
                .build();
        prepared1 = session.prepare(sql1, ph1).get();

        String sql2 = "SELECT S_ACCTBAL, S_NAME, N_NAME, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT "
                + "FROM PART, SUPPLIER, PARTSUPP, NATION, REGION "
                + "WHERE "
                + "S_SUPPKEY = PS_SUPPKEY "
                + "AND S_NATIONKEY = N_NATIONKEY "
                + "AND N_REGIONKEY = R_REGIONKEY "
                + "AND PS_PARTKEY = :partkey "
                + "AND P_SIZE = :size "
                + "AND P_TYPE3 = :type "
                + "AND R_NAME = :region "
                + "AND PS_SUPPLYCOST = :mincost "
                + "ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY";
        var ph2 = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("partkey").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("size").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("type").setType(CommonProtos.DataType.CHARACTER))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("region").setType(CommonProtos.DataType.CHARACTER))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("mincost").setType(CommonProtos.DataType.INT8))
                .build();
        prepared2 = session.prepare(sql2, ph2).get();
    }

    void q21(boolean qvalidation, Transaction transaction) throws IOException, ServerException, InterruptedException {
        for (int partkey = 1; partkey <= PARTKEY_SIZE; partkey++) {

            var ps = RequestProtos.ParameterSet.newBuilder();
            if (qvalidation) {
                ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("region").setCharacterValue("EUROPE                   "));
            } else {
                ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("region").setCharacterValue("ASIA                     "));
            }
            ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("partkey").setInt8Value(partkey));

            var future = transaction.executeQuery(prepared1, ps.getParametersList());
            var resultSet = future.get();

            try {
                if (Objects.nonNull(resultSet)) {
                    if (resultSet.nextRecord()) {
                        resultSet.nextColumn();
                        if (!resultSet.isNull()) {
                            q2intermediate.put(partkey, resultSet.getInt8());
                        }
                    } else {
                        throw new IOException("no record");
                    }
                } else {
                    throw new IOException("no resultSet");
                }
            } finally {
                if (!Objects.isNull(resultSet)) {
                    resultSet.close();
                }
                if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet.getResponse().get().getResultCase())) {
                    throw new IOException("SQL error");
                }
            }
        }
    }

    void q22(boolean qvalidation, Transaction transaction) throws IOException, ServerException, InterruptedException {
        for (Map.Entry<Integer, Long> entry : q2intermediate.entrySet()) {
            int partkey = entry.getKey();
            var ps = RequestProtos.ParameterSet.newBuilder();
            if (qvalidation) {
                ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("type").setCharacterValue("BRASS"))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("region").setCharacterValue("EUROPE                   "))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("size").setInt8Value(15));
            } else {
                ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("type").setCharacterValue("STEEL"))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("region").setCharacterValue("ASIA                     "))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("size").setInt8Value(16));
            }
            ps.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("partkey").setInt8Value(partkey))
            .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("mincost").setInt8Value(entry.getValue()));

            var future = transaction.executeQuery(prepared2, ps.getParametersList());
            var resultSet = future.get();

            try {
                if (Objects.nonNull(resultSet)) {
                    if (resultSet.nextRecord()) {
                        resultSet.nextColumn();
                        var sAcctbal = resultSet.getInt8();
                        resultSet.nextColumn();
                        var sName = resultSet.getCharacter();
                        resultSet.nextColumn();
                        var nName = resultSet.getCharacter();
                        resultSet.nextColumn();
                        var pMfgr = resultSet.getCharacter();
                        resultSet.nextColumn();
                        var sAddress = resultSet.getCharacter();
                        resultSet.nextColumn();
                        var sPhone = resultSet.getCharacter();
                        resultSet.nextColumn();
                        var sCommnent = resultSet.getCharacter();

                        System.out.println(sAcctbal + "," + sName + "," + nName + "," + partkey + "," + pMfgr + "," + sAddress + "," + sPhone + "," + sCommnent);
                    }
                } else {
                    throw new IOException("no resultSet");
                }

            } finally {
                if (!Objects.isNull(resultSet)) {
                    resultSet.close();
                }
                if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet.getResponse().get().getResultCase())) {
                    throw new IOException("SQL error");
                }
            }
        }
    }

    public void run21(Profile profile) throws IOException, ServerException, InterruptedException {
        long start = System.currentTimeMillis();
        var transaction = session.createTransaction(profile.transactionOption.build()).get();

        q21(profile.queryValidation, transaction);

        var commitResponse = transaction.commit().get();
        if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(commitResponse.getResultCase())) {
            throw new IOException("commit error");
        }
        profile.q21 = System.currentTimeMillis() - start;
    }

    public void run2(Profile profile) throws IOException, ServerException, InterruptedException {
        long start = System.currentTimeMillis();
        var transaction = session.createTransaction(profile.transactionOption.build()).get();

        q21(profile.queryValidation, transaction);
        q22(profile.queryValidation, transaction);

        var commitResponse = transaction.commit().get();
        if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(commitResponse.getResultCase())) {
            throw new IOException("commit error");
        }
        profile.q22 = System.currentTimeMillis() - start;
    }
}