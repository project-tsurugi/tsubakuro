package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import com.nautilus_technologies.tsubakuro.impl.low.connection.IpcConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;

public final class Main {
    private Main() {
    }
    
    private static String dbName = "tsubakuro";
    
    public static void main(String[] args) throws  Exception {
        long threads = 1;
        long loop = 1000000;
        int argl = args.length;

        if (argl > 0) {
            threads = Integer.parseInt(args[0]);
            if (argl > 1) {
		loop = Integer.parseInt(args[1]);
            }
	}

	new Insert(new IpcConnectorImpl(dbName), new SessionImpl()).prepareAndInsert(threads);

	ArrayList<Select> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(new Select(new IpcConnectorImpl(dbName), new SessionImpl(), i, loop));
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < tasks.size(); i++) {
            tasks.get(i).start();
        }
        for (int i = 0; i < tasks.size(); i++) {
            tasks.get(i).join();
        }
        var elapsed = System.currentTimeMillis() - start;
        System.out.printf("threads: %d loop: %d elapsed: %d ms, average: %d ns%n",
			  threads, loop, elapsed, (elapsed * 1000000) / loop);
    }
}
