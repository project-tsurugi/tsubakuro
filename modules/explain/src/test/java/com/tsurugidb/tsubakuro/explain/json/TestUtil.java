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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

final class TestUtil {

    private static final String ADAPTER_PLACEHOLDER = "\"$HERE$\"";

    static final Logger LOG = LoggerFactory.getLogger(TestUtil.class);

    static final Map<String, EdgeExtractor> DEFAULT_EDGE_EXTRACTORS = Map.of(
            "take_flat", new TakeExchangeEdgeExtractor(),
            "take_group", new TakeExchangeEdgeExtractor(),
            "take_cogroup", new TakeCogroupEdgeExtractor(),
            "join_find", new JoinExchangeEdgeExtractor(),
            "join_scan", new JoinExchangeEdgeExtractor(),
            "offer", new OfferEdgeExtractor());

    static final Predicate<Object> TRUE = t -> true;

    static JsonNode loadJson(String path) throws IOException {
        var contents = readContents(path);
        var mapper = new ObjectMapper();
        var tree = mapper.readTree(contents);
        return tree;
    }

    static PlanGraph loadPlan(String name, Map<String, PropertyExtractor> extractors)
            throws IOException, PlanGraphException {
        var json = TestUtil.loadJson(String.format("plan.files/%s.json", name));
        var analyzer = new StepGraphAnalyzer(extractors, DEFAULT_EDGE_EXTRACTORS, TRUE);
        return analyzer.analyze((ArrayNode) json);
    }

    static PlanGraph loadStatement(String name, Map<String, StatementAnalyzer> analyzers)
            throws IOException, PlanGraphException {
        String contents = readStatement(name);
        var loader = new JsonPlanGraphLoader(new ObjectMapper(), analyzers);
        return loader.load(contents);
    }

    static String readStatement(String name) throws IOException {
        var path = String.format("statement.files/%s.json", name);
        return readContents(path);
    }

    private static String readContents(String path) throws IOException {
        var resource = TestUtil.class.getResource(path);
        if (resource == null) {
            throw new FileNotFoundException(path);
        }
        try (
            var input = resource.openStream();
            var reader = new InputStreamReader(input, StandardCharsets.UTF_8);
            var writer = new StringWriter();
        ) {
            reader.transferTo(writer);
            return writer.toString();
        }
    }

    static String readPlanAsStatement(String name) throws IOException {
        var adapter = readContents("statement-adapter.json");
        var placeholderAt = adapter.indexOf(ADAPTER_PLACEHOLDER);
        if (placeholderAt < 0) {
            throw new IllegalStateException("missing placeholder");
        }

        var path = String.format("plan.files/%s.json", name);
        String replacement = readContents(path);

        var contents = adapter.substring(0, placeholderAt)
                + replacement
                + adapter.substring(placeholderAt + ADAPTER_PLACEHOLDER.length());
        return contents;
    }

    static void top(PlanNode node) {
        assertEquals(Set.of(), node.getUpstreams());
    }

    static void bottom(PlanNode node) {
        assertEquals(Set.of(), node.getDownstreams());
    }

    static void kind(PlanNode node, String kind) {
        assertEquals(kind, node.getKind());
    }

    static PlanNode next(PlanNode node) {
        return get(node.getDownstreams(), TRUE);
    }

    static PlanNode prev(PlanNode node) {
        return get(node.getDownstreams(), TRUE);
    }

    static PlanNode get(PlanGraph graph, String kind) {
        return get(graph.getNodes(), node -> Objects.equals(node.getKind(), kind));
    }

    static PlanNode get(Set<? extends PlanNode> nodes, Predicate<? super PlanNode> criteria) {
        var candidates = all(nodes, criteria);
        if (candidates.isEmpty()) {
            fail("no such element");
        }
        if (candidates.size() >= 2) {
            fail("ambiguous");
        }
        return candidates.get(0);
    }

    static List<PlanNode> all(PlanGraph graph, String kind) {
        return all(graph.getNodes(), node -> Objects.equals(node.getKind(), kind));
    }

    static List<PlanNode> all(Set<? extends PlanNode> nodes, Predicate<? super PlanNode> criteria) {
        return nodes.stream()
                .filter(criteria)
                .collect(Collectors.toList());
    }

    private TestUtil() {
        throw new AssertionError();
    }
}
