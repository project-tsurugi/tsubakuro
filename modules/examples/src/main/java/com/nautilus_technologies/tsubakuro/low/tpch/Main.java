package com.nautilus_technologies.tsubakuro.low.tpch;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

public final class Main {
    static long scale()  throws IOException, ExecutionException, InterruptedException {
	var connector = new IpcConnectorImpl(dbName);
	var session = new SessionImpl();
	session.connect(connector.connect().get());

	var transaction = session.createTransaction().get();
	var future = transaction.executeQuery("SELECT COUNT(S_SUPPKEY) FROM SUPPLIER");
	var resultSet = future.getLeft().get();
	long count = 0;
	if (resultSet.nextRecord()) {
	    if (resultSet.nextColumn()) {
		count = resultSet.getInt8();
	    }
	}
	resultSet.close();
	if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future.getRight().get().getResultCase())) {
	    throw new IOException("select error");
	}
	transaction.commit().get();
	session.close();
	return count / 10000;
    }

    private Main() {
    }

    static String dbName = "tateyama";

    public static void main(String[] args) {
	var profile = new Profile();

        // コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("l").argName("loop").hasArg().desc("Number of loops.").build());
        options.addOption(Option.builder("q").argName("query_number").hasArg().desc("Query number to be executed.").build());
        options.addOption(Option.builder("e").argName("query_string").hasArg().desc("Execute this query.").build());
        options.addOption(Option.builder("f").argName("file_contains_query").hasArg().desc("Execute the query descrived in the file specified.").build());
        options.addOption(Option.builder("v").argName("query_validation").desc("Set query validation mode.").build());
        options.addOption(Option.builder("o").argName("reqd_only_transaction").desc("Use read only transaction.").build());
        options.addOption(Option.builder("b").argName("batch(long)_transaction").desc("Use long transaction.").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
	
        try {
            cmd = parser.parse(options, args);

	    if (cmd.hasOption("o")) {
		profile.transactionOption.setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_READ_ONLY);
		System.out.println("use read only transaction");
	    }
	    if (cmd.hasOption("b")) {
		profile.transactionOption.setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_LONG);
		System.out.println("use long transaction type");
	    }
	    if (cmd.hasOption("v")) {
		profile.queryValidation = true;
	    }

	    profile.scales = scale();
	    System.out.println("benchmark started, scale = " + profile.scales);

	    var session = new SessionImpl();
	    session.connect(new IpcConnectorImpl(dbName).connect().get());

            if (cmd.hasOption("q")) {
		var queryNum = cmd.getOptionValue("q");
		if (queryNum.equals("2_1")) {
		    var query = new Q2(session);
		    query.run21(profile);
		    System.out.println("elapsed: " + profile.q21 + " mS");
		} else if (queryNum.equals("2")) {
		    var query = new Q2(session);
		    query.run2(profile);
		    System.out.println("elapsed: " + profile.q22 + " mS");
		} else if (queryNum.equals("6")) {
		    var query = new Q6(session);
		    query.run(profile);
		    System.out.println("elapsed: " + profile.q6 + " mS");
		} else if (queryNum.equals("14")) {
		    var query = new Q14(session);
		    query.run(profile);
		    System.out.println("elapsed: " + profile.q14 + " mS");
		} else if (queryNum.equals("19")) {
		    var query = new Q19(session);
		    query.run(profile);
		    System.out.println("elapsed: " + profile.q19 + " mS");
		} else {
		    System.out.println("no such query " + cmd.getOptionValue("q"));
		}
		session.close();
	    }
	} catch (IOException | ExecutionException | InterruptedException e) {
	    System.out.println(e);
	    e.printStackTrace();
        } catch (ParseException e) {
            System.err.printf("cmd parser failed." + e);
        }
    }
}
