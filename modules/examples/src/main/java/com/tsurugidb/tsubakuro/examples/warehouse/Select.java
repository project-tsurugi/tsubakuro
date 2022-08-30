package com.tsurugidb.tsubakuro.examples.warehouse;

import java.io.IOException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.ResultSet;

public class Select {
    SqlClient sqlClient;

    public Select(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    }

    void printResultset(ResultSet resultSet) throws InterruptedException, IOException, ServerException {
    int count = 1;

    while (resultSet.nextRow()) {
        System.out.println("---- ( " + count + " )----");
        count++;
        int columnIndex = 0;
        var metadata = resultSet.getMetadata().getColumns();
        while (resultSet.nextColumn()) {
            if (!resultSet.isNull()) {
                switch (metadata.get(columnIndex).getAtomType()) {
                    case INT4:
                    System.out.println(resultSet.fetchInt4Value());
                    break;
                    case INT8:
                    System.out.println(resultSet.fetchInt8Value());
                    break;
                    case FLOAT4:
                    System.out.println(resultSet.fetchFloat4Value());
                    break;
                    case FLOAT8:
                    System.out.println(resultSet.fetchFloat8Value());
                    break;
                    case CHARACTER:
                    System.out.println(resultSet.fetchCharacterValue());
                    break;
                    default:
                    throw new IOException("the column type is invalid");
                }
            } else {
                System.out.println("the column is NULL");
            }
            columnIndex++;
        }
    }
    }

    public void select() throws IOException, ServerException, InterruptedException {
        String sql = "SELECT * FROM WAREHOUSE";

        try (var transaction = sqlClient.createTransaction().await()) {
            try (var resultSet = transaction.executeQuery(sql).await()) {
                printResultset(resultSet);
                resultSet.close();
            }
            transaction.commit().await();
        }
    }
}