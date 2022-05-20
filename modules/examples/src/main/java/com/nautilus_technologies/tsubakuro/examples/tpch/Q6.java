package com.nautilus_technologies.tsubakuro.low.tpch;

import java.io.IOException;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;

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
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
            }
            } else {
            throw new IOException("no resultSet");
            }
    
            var commitResponse = transaction.commit().get();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(commitResponse.getResultCase())) {
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
