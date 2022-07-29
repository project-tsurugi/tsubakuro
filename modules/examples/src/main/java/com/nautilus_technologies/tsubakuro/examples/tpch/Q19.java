package com.nautilus_technologies.tsubakuro.examples.tpch;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class Q19 {
    SqlClient sqlClient;
    PreparedStatement prepared;

    public Q19(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    prepare();
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
    String sql =
        "SELECT SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS REVENUE "
        + "FROM LINEITEM, PART "
        + "WHERE "
        + "P_PARTKEY = L_PARTKEY "
        + "AND (( "
        + "P_BRAND = :brand1 "
        //                         "AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
        + "AND ( P_CONTAINER = 'SM CASE   ' OR  P_CONTAINER = 'SM BOX    ' OR P_CONTAINER = 'SM PACK   ' OR P_CONTAINER = 'SM PKG    ' ) "
        + "AND L_QUANTITY >= :quantity1 AND L_QUANTITY <= :quantity1 + 10 "
        //                         "AND P_SIZE BETWEEN 1 AND 5 "
        + "AND P_SIZE >= 1 AND P_SIZE <= 5 "
        //                         "AND L_SHIPMODE IN ('AIR', 'AIR REG') "
        + "AND ( L_SHIPMODE = 'AIR       ' OR  L_SHIPMODE = 'AIR REG   ' ) "
        + "AND L_SHIPINSTRUCT = 'DELIVER IN PERSON        ' "
        + ") OR ( "
        + "P_BRAND = :brand2 "
        //                         "AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
        + "AND ( P_CONTAINER = 'MED BAG   ' OR  P_CONTAINER = 'MED BOX   ' OR P_CONTAINER = 'MED PKG   ' OR P_CONTAINER = 'MED PACK  ' ) "
        + "AND L_QUANTITY >= :quantity2 AND L_QUANTITY <= :quantity2 + 10 "
        //                         "AND P_SIZE BETWEEN 1 AND 10 "
        + "AND P_SIZE >= 1 AND P_SIZE <= 10 "
        //                         "AND L_SHIPMODE IN ('AIR', 'AIR REG') "
        + "AND ( L_SHIPMODE = 'AIR       ' OR  L_SHIPMODE = 'AIR REG   ' ) "
        + "AND L_SHIPINSTRUCT = 'DELIVER IN PERSON        ' "
        + ") OR ( "
        + "P_BRAND = :brand3 "
        //                         "AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
        + "AND ( P_CONTAINER = 'LG CASE   ' OR  P_CONTAINER = 'LG BOX    ' OR P_CONTAINER = 'LG PACK   ' OR P_CONTAINER = 'LG PKG    ' ) "
        + "AND L_QUANTITY >= :quantity3 AND L_QUANTITY <= :quantity3 + 10 "
        //                         "AND P_SIZE BETWEEN 1 AND 15 "
        + "AND P_SIZE >= 1 AND P_SIZE <= 15 "
        //                         "AND L_SHIPMODE IN ('AIR', 'AIR REG') "
        + "AND ( L_SHIPMODE = 'AIR       ' OR  L_SHIPMODE = 'AIR REG   ' ) "
        + "AND L_SHIPINSTRUCT = 'DELIVER IN PERSON        ' "
        + "))";
    prepared = sqlClient.prepare(sql,
        Placeholders.of("brand1", String.class),
        Placeholders.of("quantity1", long.class),
        Placeholders.of("brand2", String.class),
        Placeholders.of("quantity2", long.class),
        Placeholders.of("brand3", String.class),
        Placeholders.of("quantity3", long.class)).get();
    }

    public void run(Profile profile) throws IOException, ServerException, InterruptedException {
        long start = System.currentTimeMillis();
        var transaction = sqlClient.createTransaction(profile.transactionOption.build()).get();
    
        var future = transaction.executeQuery(prepared,
            Parameters.of("brand1", profile.queryValidation ? "Brand#12  " : "Brand#43  "),
            Parameters.of("brand2", profile.queryValidation ? "Brand#23  " : "Brand#41  "),
            Parameters.of("brand3", profile.queryValidation ? "Brand#34  " : "Brand#35  "),
            Parameters.of("quantity1", (long) (profile.queryValidation ? 1 : 5)),
            Parameters.of("quantity2", (long) (profile.queryValidation ? 10 : 11)),
            Parameters.of("quantity3", (long) (profile.queryValidation ? 20 : 21)));
    
        try (var resultSet = future.get()) {
            if (resultSet.nextRow()) {
                resultSet.nextColumn();
                if (!resultSet.isNull()) {
                System.out.println("REVENUE " + resultSet.fetchInt8Value());
                } else {
                System.out.println("REVENUE is null");
                }
            } else {
                throw new IOException("no record");
            }
            resultSet.getResponse().get();
        } catch (ServerException e) {
            throw new IOException(e);
        }
    
        try {
            transaction.commit().get();
        } catch (ServerException e) {
            throw new IOException(e);
        } finally {
            profile.q19 = System.currentTimeMillis() - start;
        }
    }
}
