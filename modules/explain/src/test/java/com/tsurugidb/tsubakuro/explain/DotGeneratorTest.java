package com.tsurugidb.tsubakuro.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class DotGeneratorTest {

    @Test
    void quote_plain() {
        assertEquals("ok_1.2", DotGenerator.quote("ok_1.2"));
    }

    @Test
    void quote_punct() {
        assertEquals("\"a b\"", DotGenerator.quote("a b"));
        assertEquals("\"a=b\"", DotGenerator.quote("a=b"));
        assertEquals("\"a-b\"", DotGenerator.quote("a-b"));
        assertEquals("\"a:b\"", DotGenerator.quote("a:b"));
        assertEquals("\"a,b\"", DotGenerator.quote("a,b"));
    }

    @Test
    void quote_escape() {
        assertEquals("\"a\\\"b\"", DotGenerator.quote("a\"b"));
        assertEquals("\"a\\\\b\"", DotGenerator.quote("a\\b"));
    }

    @Test
    void quote_control() {
        assertEquals("\"a\\nb\"", DotGenerator.quote("a\nb"));
        assertEquals("\"a\\rb\"", DotGenerator.quote("a\rb"));
        assertEquals("\"a\\tb\"", DotGenerator.quote("a\tb"));
    }

    @Test
    void quote_unrecognized_iso_control() {
        assertEquals("\"a<NULL>b\"", DotGenerator.quote("a\000b"));
        assertEquals("\"a<BEL>b\"", DotGenerator.quote("a\007b"));
    }

    private static Set<String> collect(PlanGraph graph, DotGenerator generator) throws IOException {
        return stream(graph, generator).collect(Collectors.toSet());
    }

    private static Stream<String> stream(PlanGraph graph, DotGenerator generator) throws IOException {
        String result;
        try (var buf = new StringWriter()) {
            generator.write(graph, buf);
            result = buf.toString();
        }
        return Arrays.stream(result.split("\n"))
                .map(String::strip)
                .map(it -> it.replaceAll("\\s+", ""))
                .filter(it -> !it.isEmpty());
    }

    @Test
    void write_vertex() throws Exception {
        var node = new BasicPlanNode("a", Map.of());
        var graph = new BasicPlanGraph(List.of(node));

        var generator = new DotGenerator(List.of(), List.of(), n -> Map.of(), (m, n) -> Map.of());
        var lines = collect(graph, generator);
        assertEquals(Set.of("a_1;"), lines);
    }

    @Test
    void write_vertex_properties() throws Exception {
        var node = new BasicPlanNode("a", Map.of("label", "XXX"));
        var graph = new BasicPlanGraph(List.of(node));

        var generator = new DotGenerator(List.of(), List.of(), n -> n.getAttributes(), (m, n) -> Map.of());
        var lines = collect(graph, generator);
        assertEquals(Set.of("a_1[label=XXX];"), lines);
    }

    @Test
    void write_vertex_properties_multiple() throws Exception {
        var node = new BasicPlanNode("a", Map.of("label", "XXX", "shape", "rectangle"));
        var graph = new BasicPlanGraph(List.of(node));

        var generator = new DotGenerator(List.of(), List.of(), n -> n.getAttributes(), (m, n) -> Map.of());
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(1, lines.size());
        var line = lines.get(0);
        assertTrue(Set.of(
                "a_1[label=XXX,shape=rectangle];",
                "a_1[shape=rectangle,label=XXX];").contains(line));
    }

    @Test
    void write_edge() throws Exception {
        var a = new BasicPlanNode("a", Map.of());
        var b = new BasicPlanNode("b", Map.of());
        a.addDownstream(b);
        var graph = new BasicPlanGraph(List.of(a, b));

        var generator = new DotGenerator(List.of(), List.of(), n -> Map.of(), (m, n) -> Map.of());
        var lines = collect(graph, generator);
        assertEquals(Set.of(
                "a_1;",
                "b_1;",
                "a_1->b_1;"), lines);
    }

    @Test
    void write_edge_properties() throws Exception {
        var a = new BasicPlanNode("a", Map.of("arrowhead", "3"));
        var b = new BasicPlanNode("b", Map.of());
        a.addDownstream(b);
        var graph = new BasicPlanGraph(List.of(a, b));

        var generator = new DotGenerator(List.of(), List.of(), n -> Map.of(), (m, n) -> m.getAttributes());
        var lines = collect(graph, generator);
        assertEquals(Set.of(
                "a_1;",
                "b_1;",
                "a_1->b_1[arrowhead=3];"), lines);
    }

    @Test
    void builder_defaults() throws Exception {
        var generator = DotGenerator.newBuilder()
                .build();

        var node = new BasicPlanNode("a", "A", Map.of("p", "P"));
        var graph = new BasicPlanGraph(List.of(node));
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(List.of(
                "digraph{",
                "a_1[label=\"A\\np:P\"];",
                "}"), lines);
    }

    @Test
    void builder_header() throws Exception {
        var generator = DotGenerator.newBuilder()
                .withHeader(List.of("// testing", "digraph {"))
                .build();

        var node = new BasicPlanNode("a", Map.of());
        var graph = new BasicPlanGraph(List.of(node));
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(List.of(
                "//testing",
                "digraph{",
                "a_1[label=a];",
                "}"), lines);
    }

    @Test
    void builder_footer() throws Exception {
        var generator = DotGenerator.newBuilder()
                .withFooter(List.of("}", "// testing"))
                .build();

        var node = new BasicPlanNode("a", Map.of());
        var graph = new BasicPlanGraph(List.of(node));
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(List.of(
                "digraph{",
                "a_1[label=a];",
                "}",
                "//testing"), lines);
    }

    @Test
    void builder_show_node_kind_on() throws Exception {
        var generator = DotGenerator.newBuilder()
                .withShowNodeKind(true)
                .build();

        var node = new BasicPlanNode("a", "A", Map.of());
        var graph = new BasicPlanGraph(List.of(node));
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(List.of(
                "digraph{",
                "a_1[label=\"A\\n(a)\"];",
                "}"), lines);
    }

    @Test
    void builder_show_node_attributes_off() throws Exception {
        var generator = DotGenerator.newBuilder()
                .withShowNodeAttributes(false)
                .build();

        var node = new BasicPlanNode("a", "A", Map.of("p", "P"));
        var graph = new BasicPlanGraph(List.of(node));
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(List.of(
                "digraph{",
                "a_1[label=A];",
                "}"), lines);
    }

    @Test
    void builder_vertex_attributes() throws Exception {
        var generator = DotGenerator.newBuilder()
                .withHeader(List.of())
                .withFooter(List.of())
                .withVertexAttributes(Map.of("shape", "rectangle"))
                .build();

        var node = new BasicPlanNode("a", "A", Map.of());
        var graph = new BasicPlanGraph(List.of(node));
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(1, lines.size());
        var line = lines.get(0);
        assertTrue(Set.of(
                "a_1[label=A,shape=rectangle];",
                "a_1[shape=rectangle,label=A];").contains(line));
    }

    @Test
    void builder_vertex_attributes_overwrite_label() throws Exception {
        var generator = DotGenerator.newBuilder()
                .withVertexAttributes(Map.of("label", "OVERWRITE"))
                .build();

        var node = new BasicPlanNode("a", "A", Map.of());
        var graph = new BasicPlanGraph(List.of(node));
        var lines = stream(graph, generator).collect(Collectors.toList());
        assertEquals(List.of(
                "digraph{",
                "a_1[label=A];",
                "}"), lines);
    }

    @Test
    void builder_edge_attributes() throws Exception {
        var generator = DotGenerator.newBuilder()
                .withEdgeAttributes(Map.of("arrowhead", "3"))
                .build();

        var a = new BasicPlanNode("a", "A", Map.of());
        var b = new BasicPlanNode("b", "B", Map.of());
        a.addDownstream(b);

        var graph = new BasicPlanGraph(List.of(a, b));
        var lines = stream(graph, generator)
                .filter(it -> it.contains("->"))
                .collect(Collectors.toSet());
        assertEquals(Set.of("a_1->b_1[arrowhead=3];"), lines);
    }
}
