package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;

public class Select {
    SqlClient sqlClient;

    public Select(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    }

    void printResultset(ResultSet resultSet) throws InterruptedException, IOException {
        int count = 1;

        while (resultSet.nextRecord()) {
            System.out.println("---- ( " + count + " )----");
            count++;
            while (resultSet.nextColumn()) {
                if (!resultSet.isNull()) {
                    switch (resultSet.type()) {
                    case INT4:
                        System.out.println(resultSet.getInt4());
                        break;
                    case INT8:
                        System.out.println(resultSet.getInt8());
                        break;
                    case FLOAT4:
                        System.out.println(resultSet.getFloat4());
                        break;
                    case FLOAT8:
                        System.out.println(resultSet.getFloat8());
                        break;
                    case CHARACTER:
                        System.out.println(resultSet.getCharacter());
                        break;
                    default:
                        throw new IOException("the column type is invalid");
                    }
                } else {
                    System.out.println("the column is NULL");
                }
            }
        }
    }

    public void prepareAndSelect() throws IOException, ServerException, InterruptedException {
        String sql = "SELECT * FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id";
        try (var preparedStatement = sqlClient.prepare(sql,
        Placeholders.of("o_id", long.class),
        Placeholders.of("o_d_id", long.class),
        Placeholders.of("o_w_id", long.class)).await();

        var transaction = sqlClient.createTransaction().await()) {

            var resultSet = transaction.executeQuery(preparedStatement,
            Parameters.of("o_id", (long) 99999999),
            Parameters.of("o_d_id", (long) 3),
            Parameters.of("o_w_id", (long) 1)).get();
            if (!Objects.isNull(resultSet)) {
                printResultset(resultSet);
                resultSet.close();
            }
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet.getResponse().get().getResultCase())) {
                throw new IOException("select error");
            }
            var commitResponse = transaction.commit().get();
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                throw new IOException("commit (select) error");
            }
        }
    }
}
