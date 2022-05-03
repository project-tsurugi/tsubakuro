package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

public class Insert {
    Session session;
    PreparedStatement preparedStatement;

    public Insert(Connector connector, Session session) throws IOException, ServerException, InterruptedException {
        this.session = session;
        this.session.connect(connector.connect().await());
    }

    public void prepareAndInsert() throws IOException, ServerException, InterruptedException {
        String sql = "INSERT INTO ORDERS (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (:o_id, :o_c_id, :o_d_id, :o_w_id, :o_entry_d, :o_carrier_id, :o_ol_cnt, :o_all_local)";
        var ph = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_c_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_entry_d").setType(CommonProtos.DataType.CHARACTER))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_carrier_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_ol_cnt").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_all_local").setType(CommonProtos.DataType.INT8))
                .build();
        preparedStatement = session.prepare(sql, ph).get();

        try (Transaction transaction = session.createTransaction().await()) {
            var ps = RequestProtos.ParameterSet.newBuilder()
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(99999999))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_c_id").setInt8Value(1234))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(3))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(1))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_entry_d").setCharacterValue("20210620"))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_carrier_id").setInt8Value(3))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_ol_cnt").setInt8Value(7))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_all_local").setInt8Value(0))
                    .build();
            var result = transaction.executeStatement(preparedStatement, ps).get();
            if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result.getResultCase())) {
                if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
                    throw new IOException("error in rollback");
                }
                throw new IOException("insert error");
            }
            var commitResponse = transaction.commit().get();
            if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                throw new IOException("commit (insert) error");
            }
        } finally {
            preparedStatement.close();
            session.close();
        }
    }
}
