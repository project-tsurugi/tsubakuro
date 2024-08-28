/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.explain.json;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.List;

import com.tsurugidb.tsubakuro.explain.DotGenerator;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Generates Graphviz DOT script from explain text.
 */
public final class Main {

    /**
     * Program entry.
     * @param args {@code [0]} - input file
     * @throws IOException if I/O error was occurred
     * @throws PlanGraphException if input execution plan description was not valid
     */
    public static void main(String... args) throws IOException, PlanGraphException {
        if (args.length != 1) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "usage: java -cp ... {0} </path/to/explain.json>",
                    Main.class.getName()));
        }
        Path file = Path.of(args[0]);

        // explain result text
        var jsonText = String.join("\n", Files.readAllLines(file, StandardCharsets.UTF_8));

        // create loader
        var loader = JsonPlanGraphLoader.newBuilder()
                .build();

        // analyzes execution plan
        PlanGraph graph = loader.load(jsonText);

        // create generator
        var generator = DotGenerator.newBuilder()
                .withHeader(List.of(
                        "digraph {",
                        "rankdir=RL;",
                        "node [ shape=rectangle; ]"))
                .withShowNodeKind(true)
                .build();

        // write DOT file
        String dotText;
        try (var output = new StringWriter()) {
            generator.write(graph, output);
            dotText = output.toString();
        }

        System.out.printf("// %s%n", file.toAbsolutePath());
        System.out.print(dotText.toString());
    }

    private Main() {
        throw new AssertionError();
    }
}
