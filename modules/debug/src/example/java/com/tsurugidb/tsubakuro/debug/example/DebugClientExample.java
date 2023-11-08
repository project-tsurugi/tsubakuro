package com.tsurugidb.tsubakuro.debug.example;

import java.net.URI;

import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.debug.DebugClient;
import com.tsurugidb.tsubakuro.debug.LogLevel;

/**
 * Example program to test DebugClient
 */
public class DebugClientExample {

    private static final String DEFAULT_MESSAGE = "Tsubakuro DebugClient Test Message";

    private final URI endpoint;
    private final String message;

    DebugClientExample(String[] args) {
        this.endpoint = URI.create(args[0]);
        this.message = args.length < 2 ? DEFAULT_MESSAGE : args[1];
    }

    private void output() throws Exception {
        try (var session = SessionBuilder.connect(endpoint).withCredential(NullCredential.INSTANCE).create();
                var debug = DebugClient.attach(session)) {
            debug.logging(message);
            debug.logging(LogLevel.INFO, message);
            debug.logging(LogLevel.WARN, message);
            debug.logging(LogLevel.ERROR, message);
        }
    }

    /**
     * @param args command arguments
     * @throws Exception some exceptional situation occurred
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args[0].contains("help")) {
            System.err.println("Usage: java DebugClientExample endpoint [message]");
            System.err.println("\tex\tjava DebugClientExample ipc:tsurugi hello");
            return;
        }
        DebugClientExample app = new DebugClientExample(args);
        app.output();
    }

}
