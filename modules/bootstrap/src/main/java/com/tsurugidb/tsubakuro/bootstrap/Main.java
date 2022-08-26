package com.tsurugidb.tsubakuro.bootstrap;

import com.tsurugidb.tsubakuro.console.ScriptRunner;

/**
 * A program entry of Tsubakuro.
 */
public final class Main {

    /**
     * Executes a script file.
     * <ul>
     * <li> {@code args[0]} : path to the script file (UTF-8 encoded) </li>
     * <li> {@code args[1]} : connection URI </li>
     * </ul>
     * @param args the program arguments
     * @throws Exception if exception was occurred
     */
    public static void main(String... args) throws Exception {
        // FIXME: use options parser
        ScriptRunner.main(args);
    }

    private Main() {
        throw new AssertionError();
    }
}
