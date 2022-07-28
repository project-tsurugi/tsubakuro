package com.nautilus_technologies.tsubakuro.examples.tpch;

import java.io.IOException;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class Q14 {
    SqlClient sqlClient;
    PreparedStatement preparedT;
    PreparedStatement preparedB;

    public Q14(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    prepare();
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
    String sqlT = "SELECT "
        + "SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS MOLECULE "
        + "FROM LINEITEM, PART "
        + "WHERE "
        + "L_PARTKEY = P_PARTKEY "
        + "AND P_TYPE1 = 'PROMO' "
        + "AND L_SHIPDATE >= :datefrom "
        + "AND L_SHIPDATE < :dateto";
    preparedT = sqlClient.prepare(sqlT,
        Placeholders.of("dateto", String.class),
        Placeholders.of("datefrom", String.class)).get();

    String sqlB = "SELECT "
        + "SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS DENOMINATOR "
        + "FROM LINEITEM, PART "
        + "WHERE "
        + "L_PARTKEY = P_PARTKEY "
        + "AND L_SHIPDATE >= :datefrom "
        + "AND L_SHIPDATE < :dateto";

    preparedB = sqlClient.prepare(sqlB,
        Placeholders.of("dateto", String.class),
        Placeholders.of("datefrom", String.class)).get();
    }

    public void run(Profile profile) throws IOException, ServerException, InterruptedException {
    long start = System.currentTimeMillis();
    var transaction = sqlClient.createTransaction(profile.transactionOption.build()).get();

    long t, b;
    var futureT = transaction.executeQuery(preparedT,
        Parameters.of("datefrom", profile.queryValidation ? "1995-09-01" : "1997-11-01"),
        Parameters.of("dateto", profile.queryValidation ? "1995-10-01" : "1997-12-01"));
    var resultSetT = futureT.get();
    try {
        if (Objects.nonNull(resultSetT)) {
            if (resultSetT.nextRow()) {
                resultSetT.nextColumn();
                if (!resultSetT.isNull()) {
                    t = resultSetT.fetchInt8Value();
                } else {
                    System.out.println("REVENUE is null");
                    throw new IOException("column is null");
                }
            } else {
                throw new IOException("no record");
            }
            resultSetT.getResponse().get();
        } else {
            throw new IOException("no resultSet");
        }
    } catch (ServerException e) {
        throw new IOException(e);
    } finally {
        if (!Objects.isNull(resultSetT)) {
        resultSetT.close();
        }
    }

    var futureB = transaction.executeQuery(preparedB,
        Parameters.of("datefrom", profile.queryValidation ? "1995-09-01" : "1997-11-01"),
        Parameters.of("dateto", profile.queryValidation ? "1995-10-01" : "1997-12-01"));
    var resultSetB = futureB.get();
    try {
        if (Objects.nonNull(resultSetB)) {
            if (resultSetB.nextRow()) {
                resultSetB.nextColumn();
                if (!resultSetB.isNull()) {
                    b = resultSetB.fetchInt8Value();
                    System.out.println("PROMO_REVENUE");
                    System.out.println(t + " / " + b + " = " + (100.0 * (double) t) / (double) b + " (%)");
                } else {
                    System.out.println("REVENUE is null");
                }
            } else {
                throw new IOException("no record");
            }
            resultSetB.getResponse().get();
        } else {
            throw new IOException("no resultSet");
        }
    } catch (ServerException e) {
        throw new IOException(e);
    } finally {
        if (!Objects.isNull(resultSetB)) {
            resultSetB.close();
        }
    }

    try {
        transaction.commit().get();
    } catch (ServerException e) {
        throw new IOException(e);
    }
    profile.q14 = System.currentTimeMillis() - start;
    }
}
