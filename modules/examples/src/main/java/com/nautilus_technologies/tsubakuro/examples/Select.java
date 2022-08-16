package com.nautilus_technologies.tsubakuro.examples;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class Select {
    final SqlClient sqlClient;
    final int loopCount;
    final int selectCount;
    final int threadCount;

    static class ThreadForSelect extends Thread {
        final SqlClient sqlClient;
        final int selectCount;

        ThreadForSelect(SqlClient sqlClient, int selectCount) {
            this.sqlClient = sqlClient;
            this.selectCount = selectCount;
        }
        public void run() {
            String sql = "SELECT * FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id";
            try (var preparedStatement = sqlClient.prepare(sql,
                     Placeholders.of("o_id", long.class),
                     Placeholders.of("o_d_id", long.class),
                     Placeholders.of("o_w_id", long.class)).get(2000, TimeUnit.MILLISECONDS);

                var transaction = sqlClient.createTransaction().get(2000, TimeUnit.MILLISECONDS)) {

                for (int i = 0; i < selectCount; i++) {
                    try (var resultSet = transaction.executeQuery(preparedStatement,
                    Parameters.of("o_id", (long) 99999999),
                    Parameters.of("o_d_id", (long) 3),
                    Parameters.of("o_w_id", (long) 1)).get(2000, TimeUnit.MILLISECONDS)) {
                        printResultset(resultSet);
                    }
                }
                transaction.commit().await();

            } catch (ServerException | InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
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
    }

    public Select(SqlClient sqlClient, int loopCount, int selectCount, int threadCount) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
        this.loopCount = loopCount;
        this.selectCount = selectCount;
        this.threadCount = threadCount;
    }

    public void prepareAndSelect() throws IOException, ServerException, InterruptedException {
        Thread[] threads = new Thread[threadCount];
        for (int j = 0; j < loopCount; j++) {
            for (int i = 0; i < threadCount; i++) {
                threads[i] =  new ThreadForSelect(sqlClient, selectCount);
                threads[i].start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
            }
        }
    }
}
