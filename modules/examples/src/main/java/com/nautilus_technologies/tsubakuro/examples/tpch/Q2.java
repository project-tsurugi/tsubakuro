package com.nautilus_technologies.tsubakuro.examples.tpch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class Q2 {
    SqlClient sqlClient;
    PreparedStatement prepared1;
    PreparedStatement prepared2;
    static final int PARTKEY_SIZE = 200000;
    Map<Integer, Long> q2intermediate;

    public Q2(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
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
        prepared1 = sqlClient.prepare(sql1,
            Placeholders.of("region", String.class),
            Placeholders.of("partkey", long.class)).get();

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
        prepared2 = sqlClient.prepare(sql2,
            Placeholders.of("partkey", long.class),
            Placeholders.of("size", long.class),
            Placeholders.of("type", String.class),
            Placeholders.of("region", String.class),
            Placeholders.of("mincost", long.class)).get();
    }

    void q21(boolean qvalidation, Transaction transaction) throws IOException, ServerException, InterruptedException {
        for (int partkey = 1; partkey <= PARTKEY_SIZE; partkey++) {
            var future = transaction.executeQuery(prepared1,
                Parameters.of("region", qvalidation ? "EUROPE                   " : "ASIA                     "),
                Parameters.of("partkey", (long) partkey));
            var resultSet = future.get();

            try {
                if (Objects.nonNull(resultSet)) {
                    if (resultSet.nextRow()) {
                        resultSet.nextColumn();
                        if (!resultSet.isNull()) {
                            q2intermediate.put(partkey, resultSet.fetchInt8Value());
                        }
                    } else {
                        throw new IOException("no record");
                    }
                    resultSet.getResponse().get();
                } else {
                    throw new IOException("no resultSet");
                }
            } finally {
                if (Objects.nonNull(resultSet)) {
                    resultSet.close();
                }
            }
        }
    }

    void q22(boolean qvalidation, Transaction transaction) throws IOException, ServerException, InterruptedException {
        for (Map.Entry<Integer, Long> entry : q2intermediate.entrySet()) {
            int partkey = entry.getKey();
            var future = transaction.executeQuery(prepared2,
                Parameters.of("type", qvalidation ? "BRASS" : "STEEL"),
                Parameters.of("region", qvalidation ? "EUROPE                   " : "ASIA                     "),
                Parameters.of("size", (long) (qvalidation ? 15 : 16)),
                Parameters.of("partkey", (long) partkey),
                Parameters.of("mincost", (long) entry.getValue()));
            var resultSet = future.get();

            try {
                if (Objects.nonNull(resultSet)) {
                    if (resultSet.nextRow()) {
                        resultSet.nextColumn();
                        var sAcctbal = resultSet.fetchInt8Value();
                        resultSet.nextColumn();
                        var sName = resultSet.fetchCharacterValue();
                        resultSet.nextColumn();
                        var nName = resultSet.fetchCharacterValue();
                        resultSet.nextColumn();
                        var pMfgr = resultSet.fetchCharacterValue();
                        resultSet.nextColumn();
                        var sAddress = resultSet.fetchCharacterValue();
                        resultSet.nextColumn();
                        var sPhone = resultSet.fetchCharacterValue();
                        resultSet.nextColumn();
                        var sCommnent = resultSet.fetchCharacterValue();

                        System.out.println(sAcctbal + "," + sName + "," + nName + "," + partkey + "," + pMfgr + "," + sAddress + "," + sPhone + "," + sCommnent);
                    }
                    resultSet.getResponse().get();
                } else {
                    throw new IOException("no resultSet");
                }

            } finally {
                if (!Objects.isNull(resultSet)) {
                    resultSet.close();
                }
            }
        }
    }

    public void run21(Profile profile) throws IOException, ServerException, InterruptedException {
        long start = System.currentTimeMillis();
        var transaction = sqlClient.createTransaction(profile.transactionOption.build()).get();

        q21(profile.queryValidation, transaction);
        transaction.commit().get();

        profile.q21 = System.currentTimeMillis() - start;
    }

    public void run2(Profile profile) throws IOException, ServerException, InterruptedException {
        long start = System.currentTimeMillis();
        var transaction = sqlClient.createTransaction(profile.transactionOption.build()).get();

        q21(profile.queryValidation, transaction);
        q22(profile.queryValidation, transaction);

        transaction.commit().get();
        profile.q22 = System.currentTimeMillis() - start;
    }
}
