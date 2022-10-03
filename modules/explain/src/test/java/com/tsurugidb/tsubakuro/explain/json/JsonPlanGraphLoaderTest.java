package com.tsurugidb.tsubakuro.explain.json;

import static com.tsurugidb.tsubakuro.explain.json.TestUtil.all;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.get;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.kind;
import static com.tsurugidb.tsubakuro.explain.json.TestUtil.next;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.BasicPlanGraph;
import com.tsurugidb.tsubakuro.explain.BasicPlanNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

class JsonPlanGraphLoaderTest {

    private static PlanGraph loadPlan(String file) throws PlanGraphException, IOException {
        return loadPlan(file, JsonPlanGraphLoader.newBuilder().build());
    }

    private static PlanGraph loadPlan(String file, JsonPlanGraphLoader loader) throws PlanGraphException, IOException {
        return loader.load(TestUtil.readPlanAsStatement(file));
    }

    private static PlanGraph loadStatement(String file) throws PlanGraphException, IOException {
        return loadStatement(file, JsonPlanGraphLoader.newBuilder().build());
    }

    private static PlanGraph loadStatement(String file, JsonPlanGraphLoader loader) throws PlanGraphException, IOException {
        return loader.load(TestUtil.readStatement(file));
    }

    @Test
    void statement_execute() throws Exception {
        var graph = loadStatement("execute-emit");
        var node = get(graph, "emit");
        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void statement_write() throws Exception {
        var graph = loadStatement("write");
        var values = get(graph, "values");
        var write = next(values);
        kind(write, "write");
    }

    @Test
    void operator_find() throws Exception {
        var graph = loadPlan("find");
        var node = get(graph, "find");
        assertNotEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_scan() throws Exception {
        var graph = loadPlan("scan");
        var node = get(graph, "scan");
        assertNotEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_values() throws Exception {
        var graph = loadPlan("values");
        var node = get(graph, "values");
        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_emit() throws Exception {
        var graph = loadPlan("emit");
        var node = get(graph, "emit");
        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_write() throws Exception {
        var graph = loadPlan("write");
        var node = get(graph, "write");
        assertNotEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_join_find() throws Exception {
        var graph = loadPlan("join_find-broadcast");
        var node = get(graph, "join_find");
        assertEquals(2, node.getUpstreams().size());
        assertNotEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_join_scan() throws Exception {
        var graph = loadPlan("join_scan-broadcast");
        var node = get(graph, "join_scan");
        assertEquals(2, node.getUpstreams().size());
        assertNotEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_join_group() throws Exception {
        var graph = loadPlan("join_group");
        var node = get(graph, "join_group");
        assertNotEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_forward_exchange() throws Exception {
        var graph = loadPlan("forward-limit");
        var node = get(graph, "forward_exchange");
        assertNotEquals(Map.of(), node.getAttributes());
        assertEquals(1, node.getUpstreams().size());
        assertEquals(1, node.getDownstreams().size());
    }

    @Test
    void operator_group_exchange() throws Exception {
        var graph = loadPlan("group");
        var node = get(graph, "group_exchange");
        assertNotEquals(Map.of(), node.getAttributes());
        assertEquals(1, node.getUpstreams().size());
        assertEquals(1, node.getDownstreams().size());
    }

    @Test
    void operator_aggregate_exchange() throws Exception {
        var graph = loadPlan("aggregate");
        var node = get(graph, "aggregate_exchange");
        assertNotEquals(Map.of(), node.getAttributes());
        assertEquals(1, node.getUpstreams().size());
        assertEquals(1, node.getDownstreams().size());
    }

    @Test
    void operator_broadcast_exchange() throws Exception {
        var graph = loadPlan("broadcast");
        var node = get(graph, "broadcast_exchange");
        assertEquals(Map.of(), node.getAttributes());
        assertEquals(1, node.getUpstreams().size());
        assertEquals(1, node.getDownstreams().size());
    }

    @Test
    void operator_aggregate_group() throws Exception {
        var graph = loadPlan("aggregate_group");
        var node = get(graph, "aggregate_group");
        assertNotEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_difference_group() throws Exception {
        var graph = loadPlan("difference_group");
        var node = get(graph, "difference_group");
        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void operator_intersection_group() throws Exception {
        var graph = loadPlan("intersection_group");
        var node = get(graph, "intersection_group");
        assertEquals(Map.of(), node.getAttributes());
    }

    @Test
    void builder_object_mapper() throws Exception {
        var graph = loadStatement("comments", JsonPlanGraphLoader.newBuilder()
                .withObjectMapper(new ObjectMapper(new JsonFactory()
                        .configure(JsonParser.Feature.ALLOW_COMMENTS, true)))
                .build());
        get(graph, "write");
    }

    @Test
    void builder_register_statements() throws Exception {
        var graph = loadStatement("create_table", JsonPlanGraphLoader.newBuilder()
                .register("create_table_statement", new StatementAnalyzer() {
                    @Override
                    public PlanGraph analyze(ObjectNode node) throws PlanGraphException {
                        return new BasicPlanGraph(List.of(new BasicPlanNode("emit", Map.of())));
                    }
                })
                .build());
        get(graph, "emit");
    }

    @Test
    void builder_register_statements_replace() throws Exception {
        var graph = loadStatement("execute-write", JsonPlanGraphLoader.newBuilder()
                .register("execute_statement", new StatementAnalyzer() {
                    @Override
                    public PlanGraph analyze(ObjectNode node) throws PlanGraphException {
                        return new BasicPlanGraph(List.of(new BasicPlanNode("emit", Map.of())));
                    }
                })
                .build());
        get(graph, "emit");
    }

    @Test
    void builder_register_statements_remove() throws Exception {
        assertThrows(PlanGraphException.class, () -> loadStatement("write", JsonPlanGraphLoader.newBuilder()
                .register("write_statement", (StatementAnalyzer) null)
                .build()));
    }

    @Test
    void builder_register_operators() throws Exception {
        var graph = loadPlan("filter", JsonPlanGraphLoader.newBuilder()
                .register("filter", new ConstantPropertyExtractor("FILTER", Map.of()))
                .build());
        var node = get(graph, "filter");
        assertEquals("FILTER", node.getTitle());
    }

    @Test
    void builder_register_operators_replace() throws Exception {
        var graph = loadPlan("emit", JsonPlanGraphLoader.newBuilder()
                .register("emit", new ConstantPropertyExtractor("EMIT", Map.of()))
                .build());
        var node = get(graph, "emit");
        assertEquals("EMIT", node.getTitle());
    }

    @Test
    void builder_register_operators_remove() throws Exception {
        var graph = loadPlan("values", JsonPlanGraphLoader.newBuilder()
                .register("values", (PropertyExtractor) null)
                .build());
        assertEquals(List.of(), all(graph, "values"));
    }

    @Test
    void builder_register_edges() throws Exception {
        var graph = loadPlan("join_find-broadcast", JsonPlanGraphLoader.newBuilder()
                .register("emit", new EdgeExtractor() {
                    @Override
                    public List<String> getInputExchanges(ObjectNode object) throws PlanGraphException {
                        return List.of("@7"); // = broadcast
                    }
                })
                .build());
        var node = get(graph, "emit");
        var broadcast = get(graph, "broadcast_exchange");
        assertTrue(node.getUpstreams().contains(broadcast));
    }

    @Test
    void builder_register_edges_replace() throws Exception {
        var graph = loadPlan("join_find-broadcast", JsonPlanGraphLoader.newBuilder()
                .register("join_find", new EdgeExtractor() {
                    @Override
                    public List<String> getInputExchanges(ObjectNode object) throws PlanGraphException {
                        // suppress broadcast connection
                        return List.of();
                    }
                })
                .build());
        var node = get(graph, "broadcast_exchange");
        assertEquals(0, node.getDownstreams().size());
    }

    @Test
    void builder_register_edges_remove() throws Exception {
        var graph = loadPlan("join_find-broadcast", JsonPlanGraphLoader.newBuilder()
                .register("join_find", (EdgeExtractor) null)
                .build());
        var node = get(graph, "broadcast_exchange");
        assertEquals(0, node.getDownstreams().size());
    }

    @Test
    void builder_include() throws Exception {
        var graph = loadPlan("filter", JsonPlanGraphLoader.newBuilder()
                .withIncludeOperators(Set.of("filter"))
                .build());
        assertNotEquals(List.of(), all(graph, "filter"));
    }

    @Test
    void builder_exclude() throws Exception {
        var graph = loadPlan("values", JsonPlanGraphLoader.newBuilder()
                .withExcludeOperators(Set.of("values"))
                .build());
        assertEquals(List.of(), all(graph, "values"));
    }

    @Test
    void builder_include_exclude() throws Exception {
        var graph = loadPlan("values", JsonPlanGraphLoader.newBuilder()
                .withIncludeOperators(Set.of("values"))
                .withExcludeOperators(Set.of("values"))
                .build());
        assertEquals(List.of(), all(graph, "values"));
    }

    @Test
    void builder_node_filter() throws Exception {
        var graph = loadPlan("filter", JsonPlanGraphLoader.newBuilder()
                .withNodeFilter(n -> true)
                .build());
        assertNotEquals(List.of(), all(graph, "filter"));
    }

    @Test
    void builder_node_filter_include() throws Exception {
        var graph = loadPlan("values", JsonPlanGraphLoader.newBuilder()
                .withIncludeOperators(Set.of("values"))
                .withNodeFilter(n -> false)
                .build());
        assertNotEquals(List.of(), all(graph, "values"));
    }

    @Test
    void builder_node_filter_exclude() throws Exception {
        var graph = loadPlan("values", JsonPlanGraphLoader.newBuilder()
                .withExcludeOperators(Set.of("values"))
                .withNodeFilter(n -> true)
                .build());
        assertEquals(List.of(), all(graph, "values"));
    }

    @Test
    void load_format() throws Exception {
        var loader = JsonPlanGraphLoader.newBuilder()
                .build();
        var contents = TestUtil.readStatement("execute-emit");
        var graph = loader.load(
                JsonPlanGraphLoader.SUPPORTED_FORMAT_ID,
                JsonPlanGraphLoader.SUPPORTED_FORMAT_VERSION_MAX,
                contents);
        get(graph, "emit");
    }

    @Test
    void load_format_id_mismatch() throws Exception {
        var loader = JsonPlanGraphLoader.newBuilder()
                .build();
        var contents = TestUtil.readStatement("execute-emit");
        assertThrows(PlanGraphException.class, () -> loader.load(
                JsonPlanGraphLoader.SUPPORTED_FORMAT_ID + "=INVALID",
                JsonPlanGraphLoader.SUPPORTED_FORMAT_VERSION_MIN,
                contents));
    }

    @Test
    void load_format_version_less() throws Exception {
        var loader = JsonPlanGraphLoader.newBuilder()
                .build();
        var contents = TestUtil.readStatement("execute-emit");
        assertThrows(PlanGraphException.class, () -> loader.load(
                JsonPlanGraphLoader.SUPPORTED_FORMAT_ID,
                JsonPlanGraphLoader.SUPPORTED_FORMAT_VERSION_MIN - 1,
                contents));
    }

    @Test
    void load_format_version_greater() throws Exception {
        var loader = JsonPlanGraphLoader.newBuilder()
                .build();
        var contents = TestUtil.readStatement("execute-emit");
        assertThrows(PlanGraphException.class, () -> loader.load(
                JsonPlanGraphLoader.SUPPORTED_FORMAT_ID,
                JsonPlanGraphLoader.SUPPORTED_FORMAT_VERSION_MAX + 11,
                contents));
    }

    @Test
    void broken_json() throws Exception {
        var generator = JsonPlanGraphLoader.newBuilder()
                .build();
        var e = assertThrows(PlanGraphException.class, () -> generator.load("?"));
        assertInstanceOf(IOException.class, e.getCause());
    }
}
