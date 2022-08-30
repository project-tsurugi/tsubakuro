package com.tsurugidb.tsubakuro.examples.tpch;

import java.io.IOException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Parameters;

public class Q6 {
    SqlClient sqlClient;
    PreparedStatement prepared;

    public Q6(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
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

        prepared = sqlClient.prepare(sql,
            Placeholders.of("dateto", String.class),
            Placeholders.of("datefrom", String.class),
            Placeholders.of("discount", long.class),
            Placeholders.of("quantity", long.class)).get();
    }

    public void run(Profile profile) throws IOException, ServerException, InterruptedException {
        long start = System.currentTimeMillis();
        var transaction = sqlClient.createTransaction(profile.transactionOption.build()).get();
    
        var future = transaction.executeQuery(prepared,
            Parameters.of("datefrom", profile.queryValidation ? "1994-01-01" : "1995-01-01"),
            Parameters.of("dateto", profile.queryValidation ? "1995-01-01" : "1996-01-01"),
            Parameters.of("discount", (long) (profile.queryValidation ? 6 : 9)),
            Parameters.of("quantity", (long) (profile.queryValidation ? 24 : 25)));
    
        try (var resultSet = future.get()) {
            if (resultSet.nextRow()) {
                resultSet.nextColumn();
                if (!resultSet.isNull()) {
                System.out.println("REVENUE");
                System.out.println(resultSet.fetchInt8Value());
                } else {
                System.out.println("REVENUE is null");
                }
            }
        } catch (ServerException e) {
            throw new IOException(e);
        }
    
        try {
            transaction.commit().get();
        } catch (ServerException e) {
            throw new IOException(e);
        } finally {
            profile.q6 = System.currentTimeMillis() - start;
        }
    }
}
