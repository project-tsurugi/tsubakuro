package com.tsurugidb.tsubakuro.examples.updateExpirationTime;

import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;

public final class Main {
    private Main() {
    }

    private static String url = "ipc:tateyama";
    private static long timeout;

    public static void main(String[] args) throws Exception {
        // コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("l").argName("time length in second").hasArg().desc("time out length (s).").build());
        options.addOption(Option.builder("t").argName("stream").desc("Connect via stream endpoint.").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            
            if (cmd.hasOption("l")) {
                timeout = Long.parseLong(cmd.getOptionValue("l"));
                System.err.println("timeout = " + timeout);
            }
            if (cmd.hasOption("t")) {
                url = "tcp://localhost:12345/";
                System.err.println("connect via " + url);
            }
        } catch (ParseException e) {
            System.err.println("cmd parser failed." + e);
        }

        try (Session session = SessionBuilder.connect(url)
            .withCredential(new UsernamePasswordCredential("user", "pass"))
            .create(10, TimeUnit.SECONDS);) {
                session.updateExpirationTime(timeout, TimeUnit.SECONDS).get();
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
}
