package com.tsurugidb.tsubakuro.examples.concurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class Insert extends Thread {
    SqlClient sqlClient;
    Transaction transaction;
    PreparedStatement prepared5;

    long paramsWid = 1;
    long paramsDid = 1;
    long firstOid;
    int concurrency;

    public Insert(SqlClient sqlClient, int concurrency, long firstOid) throws IOException, ServerException, InterruptedException {
        this.concurrency = concurrency;
        this.firstOid = firstOid;
        this.sqlClient = sqlClient;
        prepare();
    }

    void prepare() throws IOException, ServerException, InterruptedException {
        String sql5 = "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id)";
        prepared5 = sqlClient.prepare(sql5,
            Placeholders.of("no_o_id", long.class),
            Placeholders.of("no_d_id", long.class),
            Placeholders.of("no_w_id", long.class)).await();
    }

    @Override
    public void run() {
        List<FutureResponse<Void>> futures = new ArrayList<>();

        try {
            if (transaction == null) {
                transaction = sqlClient.createTransaction().await();
            }

            long oid = firstOid + 1;
            int i;
            for (i = 0; i < concurrency; i++) {
                // INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
                try {
                    futures.add(transaction.executeStatement(prepared5,
                    Parameters.of("no_o_id", oid++),
                    Parameters.of("no_d_id", paramsDid),
                    Parameters.of("no_w_id", paramsWid)));
                } catch (IOException e) {
                    System.out.println(e);
                    System.out.println("The " + (i + 1) + "th and subsequent Inserts will be cancelled");
                    break;
                }
            }
            for (int j = 0; j < i; j++) {
                futures.get(j).get();
            }
            transaction.commit().get();
        } catch (IOException | ServerException | InterruptedException e) {
            System.out.println(e);
        } finally {
            try {
                prepared5.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
