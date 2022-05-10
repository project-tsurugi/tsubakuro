package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

public class Select {
    Session session;

    public Select(Connector connector, Session session) throws IOException, ServerException, InterruptedException {
        this.session = session;
        this.session.connect(connector.connect().await());
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
        var ph = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
                .build();
        try (var preparedStatement = session.prepare(sql, ph).await();
                var transaction = session.createTransaction().await()) {

            var ps = RequestProtos.ParameterSet.newBuilder()
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(99999999))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(3))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(1))
                    .build();
            var resultSet = transaction.executeQuery(preparedStatement, ps.getParametersList()).get();
            if (!Objects.isNull(resultSet)) {
                printResultset(resultSet);
                resultSet.close();
            }
            if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet.getResponse().get().getResultCase())) {
                throw new IOException("select error");
            }
            var commitResponse = transaction.commit().get();
            if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                throw new IOException("commit (select) error");
            }
        } finally {
            session.close();
        }
    }
}
