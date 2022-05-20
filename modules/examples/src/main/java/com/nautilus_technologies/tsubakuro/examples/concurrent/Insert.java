package com.nautilus_technologies.tsubakuro.low.concurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

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
        List<FutureResponse<SqlResponse.ResultOnly>> futures = new ArrayList<>();

        try {
            if (Objects.isNull(transaction)) {
                transaction = sqlClient.createTransaction().await();
            }

            long oid = firstOid + 1;
            int i;
            for (i = 0; i < concurrency; i++) {
                // INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
                try {
                    futures.add(transaction.executeStatement(prepared5,
                    Parameters.of("no_o_id", (long) oid++),
                    Parameters.of("no_d_id", (long) paramsDid),
                    Parameters.of("no_w_id", (long) paramsWid)));
                } catch (IOException e) {
                    System.out.println(e);
                    System.out.println("The " + (i + 1) + "th and subsequent Inserts will be cancelled");
                    break;
                }
            }
            for (int j = 0; j < i; j++) {
                var result5 = futures.get(j).get();
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(result5.getResultCase())) {
                    throw new IOException("error in sql");
                }
            }
            var commitResponse = transaction.commit().get();
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                throw new IOException("commit (insert) error");
            }
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
