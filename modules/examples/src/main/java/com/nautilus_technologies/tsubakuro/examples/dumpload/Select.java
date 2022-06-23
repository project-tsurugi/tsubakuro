package com.nautilus_technologies.tsubakuro.examples.dumpload;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.tsurugidb.jogasaki.proto.SqlResponse;

import java.io.IOException;
import java.util.Objects;

public class Select {
    SqlClient sqlClient;

    public Select(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    }

    static void printResultset(ResultSet resultSet) throws InterruptedException, IOException {
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

    static void prepareAndSelect(SqlClient sqlClient, String sql) throws IOException, ServerException, InterruptedException {
        try (var preparedStatement = sqlClient.prepare(sql).await();
             var transaction = sqlClient.createTransaction().await()) {
            var resultSet = transaction.executeQuery(preparedStatement).get();
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
