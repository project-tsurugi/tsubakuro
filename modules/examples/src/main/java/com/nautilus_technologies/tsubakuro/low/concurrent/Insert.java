package com.nautilus_technologies.tsubakuro.low.concurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

public class Insert extends Thread {
    Session session;
    Transaction transaction;
    PreparedStatement prepared5;

    long paramsWid = 1;
    long paramsDid = 1;
    long firstOid;
    int concurrency;

    public Insert(Connector connector, Session session, int concurrency, long firstOid) throws IOException, ServerException, InterruptedException {
        this.concurrency = concurrency;
        this.firstOid = firstOid;
        this.session = session;
        this.session.connect(connector.connect().await());
        prepare();
    }

    void prepare() throws IOException, ServerException, InterruptedException {
        String sql5 = "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id)";
        var ph5 = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_o_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_d_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_w_id").setType(CommonProtos.DataType.INT8))
                .build();
        prepared5 = session.prepare(sql5, ph5).await();
    }

    @Override
    public void run() {
        List<FutureResponse<ResponseProtos.ResultOnly>> futures = new ArrayList<>();

        try {
            if (Objects.isNull(transaction)) {
                transaction = session.createTransaction().await();
            }

            long oid = firstOid + 1;
            int i;
            for (i = 0; i < concurrency; i++) {
                // INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
                var ps5 = RequestProtos.ParameterSet.newBuilder()
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_o_id").setInt8Value(oid++))
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_d_id").setInt8Value(paramsDid))
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_w_id").setInt8Value(paramsWid))
                        .build();
                try {
                    futures.add(transaction.executeStatement(prepared5, ps5.getParametersList()));
                } catch (IOException e) {
                    System.out.println(e);
                    System.out.println("The " + (i + 1) + "th and subsequent Inserts will be cancelled");
                    break;
                }
            }
            for (int j = 0; j < i; j++) {
                var result5 = futures.get(j).get();
                if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result5.getResultCase())) {
                    throw new IOException("error in sql");
                }
            }
            var commitResponse = transaction.commit().get();
            if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                throw new IOException("commit (insert) error");
            }
        } catch (IOException | ServerException | InterruptedException e) {
            System.out.println(e);
        } finally {
            try {
                prepared5.close();
                session.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
