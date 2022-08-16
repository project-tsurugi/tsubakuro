package com.nautilus_technologies.tsubakuro.examples.tpcc;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;

public final class Customer {
    private Customer() {
    }

    public static long chooseCustomer(Transaction transaction,
            PreparedStatement prepared1, PreparedStatement prepared2,
            long paramsWid, long paramsDid, String paramsClast)
                    throws IOException, ServerException, InterruptedException {

        // SELECT COUNT(c_id) FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last"
        var future1 = transaction.executeQuery(prepared1,
        Parameters.of("c_w_id", (long) paramsWid),
        Parameters.of("c_d_id", (long) paramsDid),
        Parameters.of("c_last", paramsClast));
        long nameCnt = 0;
        try (var resultSet1 = future1.get()) {
            if (!resultSet1.nextRow()) {
                throw new IOException("no record");
            }
            resultSet1.nextColumn();
            nameCnt = resultSet1.fetchInt8Value();
            if (resultSet1.nextRow()) {
                throw new IOException("found multiple records");
            }
        } catch (ServerException e) {
            return -1;
        }

        if (nameCnt == 0) {
            return 0;
        }

        // SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first"
        var future2 = transaction.executeQuery(prepared2,
        Parameters.of("c_w_id", (long) paramsWid),
        Parameters.of("c_d_id", (long) paramsDid),
        Parameters.of("c_last", paramsClast));
        long rv = -1;
        try (var resultSet2 = future2.get()) {
            if ((nameCnt % 2) > 0) {
                nameCnt++;
            }
            for (long i = 0; i < (nameCnt / 2); i++) {
                if (!resultSet2.nextRow()) {
                    throw new IOException("no record");
                }
            }
            resultSet2.nextColumn();
            rv = resultSet2.fetchInt8Value();
            return rv;
        } catch (ServerException e) {
            return -1;
        }
    }
}
