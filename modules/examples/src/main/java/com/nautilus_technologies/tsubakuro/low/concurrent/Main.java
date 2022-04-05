package com.nautilus_technologies.tsubakuro.low.concurrent;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.channel.common.connection.ConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

public final class Main {
    static long orderId()  throws IOException, ExecutionException, InterruptedException {
        var connector = new ConnectorImpl(dbName);
        var session = new SessionImpl();
        session.connect(connector.connect().get());

        var transaction = session.createTransaction().get();
        var future = transaction.executeQuery("SELECT no_o_id FROM NEW_ORDER WHERE no_w_id = 1 AND no_d_id = 1 ORDER by no_o_id DESC");
        var resultSet = future.getLeft().get();
        long count = 0;
        if (resultSet.nextRecord()) {
            if (resultSet.nextColumn()) {
                count = resultSet.getInt8();
            }
        }
        resultSet.close();
	var r = future.getRight().get();
        if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(r.getResultCase())) {
            throw new IOException("select error");
        }
	var commitResponse = transaction.commit().get();
	if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
	    throw new IOException("commit (select) error");
	}
        session.close();
        return count;
    }

    private Main() {
    }
    
    private static String dbName = "tateyama";
    private static int concurrency = 1;
    
    public static void main(String[] args) {
	// コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("c").argName("concurrency").hasArg().desc("concurrnency level").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

	    if (cmd.hasOption("c")) {
		concurrency = Integer.parseInt(cmd.getOptionValue("c"));
	    }

	    var client = new Insert(new ConnectorImpl(dbName), new SessionImpl(), concurrency, orderId());
	    if (!Objects.isNull(client)) {
		client.start();
		client.join();
	    }
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.out.println(e);
        } catch (ParseException e) {
            System.err.printf("cmd parser failed." + e);
        }
    }
}
