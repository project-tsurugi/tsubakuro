package com.tsurugidb.tsubakuro.explain;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates Graphviz DOT style graph from execution plan graph.
 * @see #newBuilder()
 */
public class DotGenerator {

    static final Logger LOG = LoggerFactory.getLogger(DotGenerator.class);

    /**
     * The default value of DOT script header line.
     */
    public static final String DEFAULT_HEADER = "digraph {";

    /**
     * The default value of DOT script header line.
     */
    public static final String DEFAULT_FOOTER = "}";

    /**
     * The default value of whether to show the node kind.
     */
    public static final boolean DEFAULT_SHOW_NODE_KIND = false;

    /**
     * The default value of whether to show the node attributes.
     */
    public static final boolean DEFAULT_SHOW_NODE_ATTRIBUTES = true;

    private static final String ATTRIBUTE_LABEL = "label";

    private final List<String> header;

    private final List<String> footer;

    private final Function<? super PlanNode, ? extends Map<String, String>> vertexAttributes;

    private final BiFunction<? super PlanNode, ? super PlanNode, ? extends Map<String, String>> edgeAttributes;

    /**
     * Builds {@link DotGenerator}.
     */
    public static class Builder {

        private List<String> header = List.of(DEFAULT_HEADER);

        private List<String> footer = List.of(DEFAULT_FOOTER);

        private boolean showNodeKind = DEFAULT_SHOW_NODE_KIND;

        private boolean showNodeAttributes = DEFAULT_SHOW_NODE_ATTRIBUTES;

        private Map<String, String> vertexAttributes = Map.of();

        private Map<String, String> edgeAttributes = Map.of();

        /**
         * Replaces the DOT script header.
         * @param lines the header lines
         * @return this
         * @see DotGenerator#DEFAULT_HEADER
         */
        public Builder withHeader(@Nonnull List<String> lines) {
            Objects.requireNonNull(lines);
            this.header = List.copyOf(lines);
            return this;
        }

        /**
         * Replaces the DOT script footer.
         * @param lines the footer lines
         * @return this
         * @see DotGenerator#DEFAULT_FOOTER
         */
        public Builder withFooter(@Nonnull List<String> lines) {
            Objects.requireNonNull(lines);
            this.footer = List.copyOf(lines);
            return this;
        }

        /**
         * Sets whether to show node kind in each vertex label.
         * @param enabled whether to show
         * @return this
         * @see DotGenerator#DEFAULT_SHOW_NODE_KIND
         */
        public Builder withShowNodeKind(boolean enabled) {
            this.showNodeKind = enabled;
            return this;
        }

        /**
         * Sets whether to show node attributes in each vertex label.
         * @param enabled whether to show
         * @return this
         * @see DotGenerator#DEFAULT_SHOW_NODE_ATTRIBUTES
         */
        public Builder withShowNodeAttributes(boolean enabled) {
            this.showNodeAttributes = enabled;
            return this;
        }

        /**
         * Sets attributes for individual vertices.
         * Note that, {@code label} attribute will be replaced even if it is specified.
         * @param attributes the vertex attributes
         * @return this
         */
        public Builder withVertexAttributes(@Nonnull Map<String, String> attributes) {
            Objects.requireNonNull(attributes);
            this.vertexAttributes = Map.copyOf(attributes);
            if (this.vertexAttributes.containsKey(ATTRIBUTE_LABEL)) {
                LOG.warn(MessageFormat.format(
                        "vertex attribute \"{0}\" is ignored",
                        ATTRIBUTE_LABEL));
            }
            return this;
        }

        /**
         * Sets attributes for individual edges.
         * Note that, {@code label} attribute will be replaced even if it is specified.
         * @param attributes the vertex attributes
         * @return this
         */
        public Builder withEdgeAttributes(Map<String, String> attributes) {
            Objects.requireNonNull(attributes);
            this.edgeAttributes = Map.copyOf(attributes);
            return this;
        }

        /**
         * Creates a new {@link DotGenerator}.
         * @return the created instance.
         */
        public DotGenerator build() {
            return new DotGenerator(
                    header,
                    footer,
                    new VertexAttributeProvider(showNodeKind, showNodeAttributes, vertexAttributes),
                    (from, to) -> edgeAttributes);
        }
    }

    private static class VertexAttributeProvider implements Function<PlanNode, Map<String, String>> {

        private final boolean showNodeKind;

        private final boolean showNodeAttributes;

        private final Map<String, String> extraAttributes;

        VertexAttributeProvider(
                boolean showNodeKind,
                boolean showNodeAttributes,
                Map<String, String> extraAttributes) {
            this.showNodeKind = showNodeKind;
            this.showNodeAttributes = showNodeAttributes;
            this.extraAttributes = extraAttributes;
        }

        @Override
        public Map<String, String> apply(PlanNode node) {
            var results = new LinkedHashMap<String, String>();
            results.put(ATTRIBUTE_LABEL, computeLabel(node));
            extraAttributes.forEach(results::putIfAbsent);
            return results;
        }

        private String computeLabel(PlanNode node) {
            var buf = new StringBuilder();
            buf.append(node.getTitle());
            if (showNodeKind) {
                buf.append('\n');
                buf.append('(');
                buf.append(node.getKind());
                buf.append(')');
            }
            if (showNodeAttributes) {
                node.getAttributes().forEach((k, v) -> buf
                        .append('\n')
                        .append(String.format("%s: %s", k, v))); //$NON-NLS-1$
            }
            return buf.toString();
        }
    }

    /**
     * Creates a new builder for {@link DotGenerator}.
     * @return the created builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a new instance.
     * @param header the DOT file header
     * @param footer the DOT file footer
     * @param vertexAttributes vertex attributes
     * @param edgeAttributes edge attributes
     * @see #newBuilder()
     */
    public DotGenerator(
            @Nonnull List<String> header,
            @Nonnull List<String> footer,
            @Nonnull Function<? super PlanNode, ? extends Map<String, String>> vertexAttributes,
            @Nonnull BiFunction<? super PlanNode, ? super PlanNode, ? extends Map<String, String>> edgeAttributes) {
        Objects.requireNonNull(header);
        Objects.requireNonNull(footer);
        Objects.requireNonNull(vertexAttributes);
        Objects.requireNonNull(edgeAttributes);
        this.header = List.copyOf(header);
        this.footer = List.copyOf(footer);
        this.vertexAttributes = vertexAttributes;
        this.edgeAttributes = edgeAttributes;
    }

    /**
     * Writes DOT script into the output.
     * @param graph the source execution plan graph
     * @param output the destination output
     * @throws IOException if I/O error was occurred while output
     */
    public void write(@Nonnull PlanGraph graph, @Nonnull Appendable output) throws IOException {
        Objects.requireNonNull(output);
        Objects.requireNonNull(graph);
        var nodes = PlanGraphUtil.sort(graph.getNodes());
        var identifiers = computeIdentifiers(nodes);

        for (var line : header) {
            append(output, line);
        }
        printBody(output, nodes, identifiers);
        for (var line : footer) {
            append(output, line);
        }
    }

    private static Map<PlanNode, String> computeIdentifiers(List<PlanNode> nodes) {
        var results = new HashMap<PlanNode, String>();
        var counters = new HashMap<String, Counter>();
        for (var node : nodes) {
            var key = node.getKind();
            var counter = counters.computeIfAbsent(key, k -> new Counter());
            counter.value++;
            results.put(node, String.format("%s_%d", key, counter.value)); //$NON-NLS-1$
        }
        return results;
    }

    private void printBody(Appendable output, List<PlanNode> nodes, Map<PlanNode, String> identifiers)
            throws IOException {
        for (var node : nodes) {
            var id = identifiers.get(node);
            var attributes = vertexAttributes.apply(node);
            if (attributes.isEmpty()) {
                append(output, String.format("%s;", id)); //$NON-NLS-1$
            } else {
                append(output, String.format("%s [%s];", id, toString(attributes))); //$NON-NLS-1$
            }
        }
        for (var upstream : nodes) {
            var upstreamId = identifiers.get(upstream);
            for (var downstream : upstream.getDownstreams()) {
                var downstreamId = identifiers.get(downstream);
                var attributes = edgeAttributes.apply(upstream, downstream);
                if (attributes.isEmpty()) {
                    append(output, String.format("%s -> %s;", upstreamId, downstreamId)); //$NON-NLS-1$
                } else {
                    append(output, String.format("%s -> %s [%s];", upstreamId, downstreamId, toString(attributes))); //$NON-NLS-1$
                }
            }
        }
    }

    static String toString(Map<String, String> attributes) {
        var buf = new StringBuffer();
        for (var entry : attributes.entrySet()) {
            if (buf.length() > 0) {
                buf.append(',');
            }
            buf.append(quote(entry.getKey()));
            buf.append('=');
            buf.append(quote(entry.getValue()));
        }
        return buf.toString();
    }

    static String quote(String text) {
        boolean required = false;
        var buf = new StringBuilder();
        buf.append('"');
        for (int i = 0, n = text.length(); i < n; i++) {
            var c = text.charAt(i);
            if (('0' <= c && c <= '9')
                    || ('A' <= c && c <= 'Z')
                    || ('a' <= c && c <= 'z')
                    || c == '_'
                    || c == '.') {
                buf.append(c);
                continue;
            }
            required = true;
            switch (c) {
            case '\\':
            case '"':
                buf.append('\\').append(c);
                break;

            case '\r':
                buf.append('\\').append('r');
                break;

            case '\n':
                buf.append('\\').append('n');
                break;

            case '\t':
                buf.append('\\').append('t');
                break;

            default:
                if (Character.isISOControl(c)) {
                    buf.append('<').append(Character.getName(c)).append('>');
                } else {
                    buf.append(c);
                }
                break;
            }
        }
        if (!required) {
            return text;
        }
        buf.append('"');
        return buf.toString();
    }

    private static void append(Appendable output, String line) throws IOException {
        LOG.trace(line);
        output.append(line);
        output.append('\n');
    }

    private static class Counter {
        int value;
    }
}
