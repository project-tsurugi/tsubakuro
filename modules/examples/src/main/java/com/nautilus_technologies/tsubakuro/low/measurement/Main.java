package com.nautilus_technologies.tsubakuro.low.measurement;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
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
    static long warehouses()  throws IOException, ExecutionException, InterruptedException {
        var connector = new ConnectorImpl(dbName);
        var session = new SessionImpl();
        session.connect(connector.connect().get());

        var transaction = session.createTransaction().get();
        var future = transaction.executeQuery("SELECT COUNT(w_id) FROM WAREHOUSE");
        var resultSet = future.get();
        long count = 0;
        if (resultSet.nextRecord()) {
            if (resultSet.nextColumn()) {
                count = resultSet.getInt8();
            }
        }
        resultSet.close();
        if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet.getResponse().get().getResultCase())) {
            throw new IOException("select error");
        }
        transaction.commit().get();
        session.close();
        return count;
    }

    private Main() {
    }
    
    enum Type {
	SELECT_ONE,
	SELECT_MULTI,
	SELECT_LIMIT_ONE,
	INSERT,
	UPDATE,
    };

    private static String dbName = "tateyama";
    private static int pattern = 1;
    private static int duration = 30;
    private static Type type = Type.SELECT_ONE;

    public static void main(String[] args) {
	// コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("d").argName("duration").hasArg().desc("duration in seconds").build());
        options.addOption(Option.builder("t").argName("SQL type").hasArg().desc("type of SQL").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

	    if (cmd.hasOption("d")) {
		duration = Integer.parseInt(cmd.getOptionValue("d"));
	    }
	    if (cmd.hasOption("t")) {
		var givenType = cmd.getOptionValue("t");
		if (givenType.equals("select")) {
		    type = Type.SELECT_ONE;
		} else if (givenType.equals("selectm")) {
		    type = Type.SELECT_MULTI;
		} else if (givenType.equals("selectl")) {
		    type = Type.SELECT_LIMIT_ONE;
		} else if (givenType.equals("insert")) {
		    type = Type.INSERT;
		} else if (givenType.equals("update")) {
		    type = Type.UPDATE;
		} else {
		    throw new ParseException("illegal type");
		}
	    }

            var warehouses = warehouses();
            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicBoolean stop = new AtomicBoolean();
	    var profile = new Profile(warehouses);

	    Thread client = null;
	    switch (type) {
	    case SELECT_ONE:
		client = new SelectOne(new ConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop);
		break;
	    case SELECT_MULTI:
		client = new SelectMulti(new ConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop);
		break;
	    case SELECT_LIMIT_ONE:
		client = new SelectLimitOne(new ConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop);
		break;
	    case INSERT:
		client = new Insert(new ConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop);
		break;
	    case UPDATE:
		client = new Update(new ConnectorImpl(dbName), new SessionImpl(), profile, barrier, stop);
		break;
	    default:
		System.out.println("illegal type");
		return;
	    }
	    if (!Objects.isNull(client)) {
		client.start();
		barrier.await();
		System.out.println("benchmark started, warehouse = " + warehouses);
		Thread.sleep(duration * 1000);
		stop.set(true);
		client.join();
		System.out.println("benchmark stoped");
		profile.print();
	    }
        } catch (IOException e) {
            System.out.println(e);
        } catch (ExecutionException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        } catch (BrokenBarrierException e) {
            System.out.println(e);
        } catch (ParseException e) {
            System.err.printf("cmd parser failed." + e);
        }
    }
}
