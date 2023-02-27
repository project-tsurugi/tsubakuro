package com.tsurugidb.tsubakuro.explain.json;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.BasicPlanGraph;
import com.tsurugidb.tsubakuro.explain.BasicPlanNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanNode;

/**
 * Extracts step graph information from JSON style step graph descriptors.
 */
public class StepGraphAnalyzer {

    static final Logger LOG = LoggerFactory.getLogger(StepGraphAnalyzer.class);

    private static final String K_OPERATORS = "operators"; //$NON-NLS-1$

    private static final String K_OUTPUT_PORTS = "output_ports"; //$NON-NLS-1$

    private static final String K_OPPOSITE = "opposite"; //$NON-NLS-1$

    private static final String KIND_SUFFIX_EXCHANGE = "_exchange"; //$NON-NLS-1$

    private static final PropertyExtractor DEFAULT_EXTRACTOR = new DefaultPropertyExtractor();

    private final Map<String, PropertyExtractor> properties;

    private final Map<String, EdgeExtractor> edges;

    private final Predicate<? super PlanNode> nodeFilter;

    /**
     * Creates a new instance.
     * @param properties node property extractors
     * @param edges node edge extractors
     * @param nodeFilter predicate to test whether keep individual operators
     */
    public StepGraphAnalyzer(
            @Nonnull Map<String, ? extends PropertyExtractor> properties,
            @Nonnull Map<String, ? extends EdgeExtractor> edges,
            @Nonnull Predicate<? super PlanNode> nodeFilter) {
        Objects.requireNonNull(properties);
        Objects.requireNonNull(edges);
        Objects.requireNonNull(nodeFilter);
        this.properties = Map.copyOf(properties);
        this.edges = Map.copyOf(edges);
        this.nodeFilter = nodeFilter;
    }

    /**
     * Extracts step graph from array node of step list.
     * @param node the array node
     * @return the extracted plan graph
     * @throws PlanGraphException if error was occurred while extracting step graph
     */
    public PlanGraph analyze(@Nonnull ArrayNode node) throws PlanGraphException {
        Objects.requireNonNull(node);
        var steps = new ArrayList<Step>();
        for (var element : node) {
            var step = analyzeStep(element);
            steps.add(step);
        }
        connectSteps(steps);

        var nodes = new ArrayList<BasicPlanNode>();
        for (var step : steps) {
            for (var element : step.elements) {
                if (nodeFilter.test(element)) {
                    nodes.add(element);
                } else {
                    element.bypass();
                }
            }
        }
        return new BasicPlanGraph(nodes);
    }

    private Step analyzeStep(JsonNode node) throws PlanGraphException {
        var kind = JsonUtil.getKind(node);
        if (Objects.equals(kind, "process")) {
            return analyzeProcess((ObjectNode) node);
        }
        return analyzeExchange((ObjectNode) node);
    }

    private Step analyzeProcess(ObjectNode node) throws PlanGraphException {
        var self = JsonUtil.getThis(node);
        LOG.trace("analyze exchange: this={}", self); //$NON-NLS-1$

        var operators = collectOperators(node);
        connectOperators(self, operators);

        var elements = operators.stream()
                .map(it -> it.element)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        var inputs = new LinkedHashMap<String, List<BasicPlanNode>>();
        var outputs = new LinkedHashMap<String, List<BasicPlanNode>>();
        for (var operator : operators) {
            for (var key : operator.inputs) {
                inputs.computeIfAbsent(key, k -> new ArrayList<>()).add(operator.element);
            }
            for (var key : operator.outputs) {
                outputs.computeIfAbsent(key, k -> new ArrayList<>()).add(operator.element);
            }
        }
        return Step.process(elements, inputs, outputs);
    }

    private List<Operator> collectOperators(ObjectNode node) throws PlanGraphException {
        var results = new ArrayList<Operator>();
        var operators = JsonUtil.getArray(node, K_OPERATORS);
        for (var op : operators) {
            JsonUtil.getKind(node);
            var analyzed = analyzeOperator((ObjectNode) op);
            results.add(analyzed);
        }
        return results;
    }

    private Operator analyzeOperator(ObjectNode node) throws PlanGraphException {
        var kind = JsonUtil.getKind(node);
        var self = JsonUtil.getThis(node);
        LOG.trace("analyze relational operator: kind={}, this={}", kind, self); //$NON-NLS-1$

        var propertyExtractor = properties.getOrDefault(kind, DEFAULT_EXTRACTOR);
        LOG.trace("using property extractor for operator: kind={}, extractor={}", kind, propertyExtractor); //$NON-NLS-1$

        var title = propertyExtractor.getTitle(node).orElse(kind);
        var attributes = propertyExtractor.getAttributes(node);
        var element = new BasicPlanNode(kind, title, attributes);

        var edgeExtractor = edges.get(kind);
        if (edgeExtractor == null) {
            LOG.trace("no edge extractor for operator: kind={}", kind); //$NON-NLS-1$
            return new Operator(self, node, element, List.of(), List.of());
        }
        LOG.trace("using edge extractor for operator: kind={}, extractor={}", kind, edgeExtractor); //$NON-NLS-1$
        var inputs = edgeExtractor.getInputExchanges(node);
        var outputs = edgeExtractor.getOutputExchanges(node);
        return new Operator(self, node, element, inputs, outputs);
    }

    private void connectOperators(String process, List<Operator> operators) throws PlanGraphException {
        var map = new HashMap<String, Operator>();
        operators.forEach(it -> map.put(it.reference, it));
        for (var operator : operators) {
            var outputs = JsonUtil.getArray(operator.json, K_OUTPUT_PORTS);
            for (var output : outputs) {
                var opposite = JsonUtil.getObject(output, K_OPPOSITE);
                var oppositeRef = JsonUtil.getThis(opposite);
                var oppositeNode = map.get(oppositeRef);
                if (oppositeNode == null) {
                    throw new PlanGraphException(MessageFormat.format(
                            "missing successing operator: \"{0}\" (in \"{1}\")",
                            oppositeRef,
                            process));
                }
                operator.element.addDownstream(oppositeNode.element);
            }
        }
    }

    private Step analyzeExchange(ObjectNode node) throws PlanGraphException {
        var kind = JsonUtil.getKind(node) + KIND_SUFFIX_EXCHANGE;
        var self = JsonUtil.getThis(node);
        LOG.trace("analyze exchange: kind={}, this={}", kind, self); //$NON-NLS-1$

        var extractor = properties.getOrDefault(kind, DEFAULT_EXTRACTOR);
        LOG.trace("using extractor for exchange: kind={}, extractor={}", kind, extractor); //$NON-NLS-1$

        var title = extractor.getTitle(node).orElse(kind);
        var attributes = extractor.getAttributes(node);
        return Step.exchange(self, new BasicPlanNode(kind, title, attributes));
    }

    private void connectSteps(List<Step> steps) {
        var inputs = new HashMap<String, List<BasicPlanNode>>();
        var outputs = new HashMap<String, List<BasicPlanNode>>();

        // collect exchange references
        for (var step : steps) {
            if (step.kind == StepKind.EXCHANGE) {
                step.inputs.forEach((k, v) -> {
                    inputs.computeIfAbsent(k, k2 -> new ArrayList<>()).addAll(v);
                });
                step.outputs.forEach((k, v) -> {
                    outputs.computeIfAbsent(k, k2 -> new ArrayList<>()).addAll(v);
                });
            }
        }

        // connect between exchanges and process edges
        for (var step : steps) {
            if (step.kind == StepKind.PROCESS) {
                for (var entry : step.inputs.entrySet()) {
                    var opposites = outputs.getOrDefault(entry.getKey(), List.of());
                    if (opposites.isEmpty()) {
                        LOG.warn(MessageFormat.format(
                                "missing upstream exchange in execution plan: {0}",
                                entry.getKey()));
                    } else {
                        for (var opposite : opposites) {
                            for (var element : entry.getValue()) {
                                opposite.addDownstream(element);
                            }
                        }
                    }
                }
                for (var entry : step.outputs.entrySet()) {
                    var opposites = outputs.getOrDefault(entry.getKey(), List.of());
                    if (opposites.isEmpty()) {
                        LOG.warn(MessageFormat.format(
                                "missing downstream exchange in execution plan: {0}",
                                entry.getKey()));
                    } else {
                        for (var opposite : opposites) {
                            for (var element : entry.getValue()) {
                                opposite.addUpstream(element);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return String.format("StepGraphAnalyzer(%s)", properties.keySet().stream()
                .sorted()
                .collect(Collectors.toList()));
    }

    private static class Operator {

        final String reference;

        final ObjectNode json;

        final BasicPlanNode element;

        final List<String> inputs;

        final List<String> outputs;

        Operator(String reference, ObjectNode json, BasicPlanNode element, List<String> inputs, List<String> outputs) {
            this.reference = reference;
            this.json = json;
            this.element = element;
            this.inputs = inputs;
            this.outputs = outputs;
        }
    }

    enum StepKind {
        PROCESS,
        EXCHANGE,
    }

    private static final class Step {

        final StepKind kind;

        final Set<BasicPlanNode> elements;

        final Map<String, List<BasicPlanNode>> inputs;

        final Map<String, List<BasicPlanNode>> outputs;

        static Step process(
                Set<BasicPlanNode> elements,
                Map<String, List<BasicPlanNode>> inputs,
                Map<String, List<BasicPlanNode>> outputs) {
            return new Step(StepKind.PROCESS, elements, inputs, outputs);
        }

        static Step exchange(String reference, BasicPlanNode node) {
            return new Step(
                    StepKind.EXCHANGE,
                    Set.of(node),
                    Map.of(reference, List.of(node)),
                    Map.of(reference, List.of(node)));
        }

        private Step(
                StepKind kind,
                Set<BasicPlanNode> elements,
                Map<String, List<BasicPlanNode>> inputs,
                Map<String, List<BasicPlanNode>> outputs) {
            this.kind = kind;
            this.elements = elements;
            this.inputs = inputs;
            this.outputs = outputs;
        }
    }
}
