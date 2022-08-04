package com.nautilus_technologies.tsubakuro.examples;

import java.io.IOException;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

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

    public void prepareAndSelect(int selectCount) throws IOException, ServerException, InterruptedException {
        String sql = "SELECT * FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id";
        try (var preparedStatement = sqlClient.prepare(sql,
                 Placeholders.of("o_id", long.class),
                 Placeholders.of("o_d_id", long.class),
                 Placeholders.of("o_w_id", long.class)).await();

            var transaction = sqlClient.createTransaction().await()) {

                for (int i = 0; i < selectCount; i++) {
                    try (var resultSet = transaction.executeQuery(preparedStatement,
                    Parameters.of("o_id", (long) 99999999),
                    Parameters.of("o_d_id", (long) 3),
                    Parameters.of("o_w_id", (long) 1)).await()) {
                        if (!Objects.isNull(resultSet)) {
                            printResultset(resultSet);
                                resultSet.close();
                        }
                        resultSet.getResponse().await();
                    }
                }

            transaction.commit().await();
        }
    }
}
