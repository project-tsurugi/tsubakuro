package com.nautilus_technologies.tsubakuro.examples;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class Insert {
    SqlClient sqlClient;
    PreparedStatement preparedStatement;

    public Insert(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    }

    public void prepareAndInsert() throws IOException, ServerException, InterruptedException {
        String sql = "INSERT INTO ORDERS (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (:o_id, :o_c_id, :o_d_id, :o_w_id, :o_entry_d, :o_carrier_id, :o_ol_cnt, :o_all_local)";
        preparedStatement = sqlClient.prepare(sql,
        Placeholders.of("o_id", long.class),
        Placeholders.of("o_c_id", long.class),
        Placeholders.of("o_d_id", long.class),
        Placeholders.of("o_w_id", long.class),
        Placeholders.of("o_entry_d", String.class),
        Placeholders.of("o_carrier_id", long.class),
        Placeholders.of("o_ol_cnt", long.class),
        Placeholders.of("o_all_local", long.class)).get();

        try (Transaction transaction = sqlClient.createTransaction().await()) {
            try {
                var result = transaction.executeStatement(preparedStatement,
                    Parameters.of("o_id", (long) 99999999),
                    Parameters.of("o_c_id", (long) 1234),
                    Parameters.of("o_d_id", (long) 3),
                    Parameters.of("o_w_id", (long) 1),
                    Parameters.of("o_entry_d", "20210620"),
                    Parameters.of("o_carrier_id", (long) 3),
                    Parameters.of("o_ol_cnt", (long) 7),
                    Parameters.of("o_all_local", (long) 0)).get();
                transaction.commit().get();
            } catch (ServerException e) {
                transaction.rollback().get();
            }
        } finally {
            preparedStatement.close();
        }
    }
}
