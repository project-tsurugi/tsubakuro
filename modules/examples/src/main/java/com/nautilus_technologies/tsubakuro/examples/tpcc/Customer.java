package com.nautilus_technologies.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.tsurugidb.jogasaki.proto.SqlResponse;

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
        var resultSet1 = future1.get();
        try {
            if (!Objects.isNull(resultSet1)) {
                if (!resultSet1.nextRow()) {
                    if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet1.getResponse().get().getResultCase())) {
                        throw new IOException("SQL error");
                    }
                    throw new IOException("no record");
                }
                resultSet1.nextColumn();
                nameCnt = resultSet1.fetchInt8Value();
                if (resultSet1.nextRow()) {
                    if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet1.getResponse().get().getResultCase())) {
                        throw new IOException("SQL error");
                    }
                    throw new IOException("found multiple records");
                }
            }
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet1.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
            }
        } catch (ServerException e) {
            return -1;
        } finally {
            if (!Objects.isNull(resultSet1)) {
                resultSet1.close();
                resultSet1 = null;
            }
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
        var resultSet2 = future2.get();
        try {
            if (!Objects.isNull(resultSet2)) {
                if ((nameCnt % 2) > 0) {
                    nameCnt++;
                }
                for (long i = 0; i < (nameCnt / 2); i++) {
                    if (!resultSet2.nextRow()) {
                        if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
                            throw new IOException("SQL error");
                        }
                        throw new IOException("no record");
                    }
                }
                resultSet2.nextColumn();
                rv = resultSet2.fetchInt8Value();
            }
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
            }
            return rv;
        } catch (ServerException e) {
            return -1;
        } finally {
            if (!Objects.isNull(resultSet2)) {
                resultSet2.close();
                resultSet2 = null;
            }
        }
    }
}
