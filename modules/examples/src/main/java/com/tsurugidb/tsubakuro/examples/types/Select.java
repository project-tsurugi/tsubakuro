package com.tsurugidb.tsubakuro.examples.types;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;

import java.io.IOException;

public class Select {
    SqlClient sqlClient;

    public Select(SqlClient sqlClient) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    }

    static void printResultset(ResultSet resultSet) throws InterruptedException, IOException, ServerException {
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
                        case DECIMAL:
                            System.out.println(resultSet.fetchDecimalValue());
                            break;
                        case DATE:
                            System.out.println(resultSet.fetchDateValue());
                            break;
                        case TIME_OF_DAY:
                            System.out.println(resultSet.fetchTimeOfDayValue());
                            break;
                        case TIME_OF_DAY_WITH_TIME_ZONE:
                            System.out.println(resultSet.fetchTimeOfDayWithTimeZoneValue());
                            break;
                        case TIME_POINT:
                            System.out.println(resultSet.fetchTimePointValue());
                            break;
                        case TIME_POINT_WITH_TIME_ZONE:
                            System.out.println(resultSet.fetchTimePointWithTimeZoneValue());
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

    static void prepareAndSelect(SqlClient sqlClient, String sql) throws IOException, ServerException, InterruptedException {
        try (var preparedStatement = sqlClient.prepare(sql).await();
             var transaction = sqlClient.createTransaction().await()) {
            try (var resultSet = transaction.executeQuery(preparedStatement).get()) {
                printResultset(resultSet);
            }
            transaction.commit().get();
        }
    }
}
