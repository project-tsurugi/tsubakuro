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

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanGraphLoader;
import com.tsurugidb.tsubakuro.explain.PlanNode;

/**
 * An implementation of {@link PlanGraphLoader} that extracts execution plan information
 * from JSON style statement descriptors.
 */
public class JsonPlanGraphLoader implements PlanGraphLoader {

    static final Logger LOG = LoggerFactory.getLogger(JsonPlanGraphLoader.class);

    /**
     * The explain content format ID which this implementation supports.
     */
    public static final String SUPPORTED_FORMAT_ID = "jogasaki-statement.json"; //$NON-NLS-1$

    /**
     * The minimum format version number which this this implementation supports.
     * @see #SUPPORTED_FORMAT_VERSION_MIN
     */
    public static final long SUPPORTED_FORMAT_VERSION_MIN = 0;

    /**
     * The maximum format version number which this this implementation supports.
     * @see #SUPPORTED_FORMAT_VERSION_MIN
     */
    public static final long SUPPORTED_FORMAT_VERSION_MAX = 1;

    private static final String KIND_SUFFIX_STATEMENT = "_statement"; //$NON-NLS-1$

    private static final Map<String, Function<Config, StatementAnalyzer>> DEFAULT_STATEMENT_ANALYZERS = Map.ofEntries(
            Map.entry("execute_statement", conf -> new ExecuteStatementAnalyzer(conf.stepGraphAnalyzer)),
            Map.entry("write_statement", conf -> new WriteStatementAnalyzer(conf.nodeFilter)));

    private static final Map<String, PropertyExtractor> DEFAULT_PROPERTY_EXTRACTORS = Map.ofEntries(
            // input operators
            Map.entry("find", new FindPropertyExtractor()),
            Map.entry("scan", new ScanPropertyExtractor()),
            Map.entry("values", new DefaultPropertyExtractor()),

            // output operators
            Map.entry("emit", new DefaultPropertyExtractor()),
            Map.entry("write", new WritePropertyExtractor()),

            // join operators
            Map.entry("join_find", new JoinFindPropertyExtractor()),
            Map.entry("join_scan", new JoinScanPropertyExtractor()),
            Map.entry("join_group", new JoinGroupPropertyExtractor()),

            // exchange operators
            Map.entry("forward_exchange", new ForwardExchangePropertyExtractor()),
            Map.entry("group_exchange", new GroupExchangePropertyExtractor()),
            Map.entry("aggregate_exchange", new AggregateExchangePropertyExtractor()),
            Map.entry("broadcast_exchange", new DefaultPropertyExtractor()),

            // other operators
            Map.entry("aggregate_group", new ConstantPropertyExtractor("aggregate", Map.of("incremental", "false"))),
            Map.entry("difference_group", new DefaultPropertyExtractor()),
            Map.entry("intersection_group", new DefaultPropertyExtractor()));

    private static final Map<String, EdgeExtractor> DEFAULT_EDGE_EXTRACTORS = Map.of(
            "take_flat", new TakeExchangeEdgeExtractor(),
            "take_group", new TakeExchangeEdgeExtractor(),
            "take_cogroup", new TakeCogroupEdgeExtractor(),
            "join_find", new JoinExchangeEdgeExtractor(),
            "join_scan", new JoinExchangeEdgeExtractor(),
            "offer", new OfferEdgeExtractor());

    /**
     * A builder for {@link JsonPlanGraphLoader}.
     */
    public static class Builder {

        private @Nullable ObjectMapper objectMapper;

        private final Map<String, StatementAnalyzer> statementAnalyzers = new HashMap<>();

        private final Map<String, PropertyExtractor> propertyExtractors = new HashMap<>();

        private final Map<String, EdgeExtractor> edgeExtractors = new HashMap<>();

        private final Set<String> includeOperatorKinds = new HashSet<>();

        private final Set<String> excludeOperatorKinds = new HashSet<>();

        private @Nullable Predicate<? super PlanNode> nodeFilter;

        /**
         * Sets JSON analyzer.
         * <p>
         * This is for configuring behavior of input JSON text parser (e.g. accepts JSON with comments).
         * </p>
         * @param mapper JSON analyzer
         * @return this
         */
        public Builder withObjectMapper(@Nonnull ObjectMapper mapper) {
            Objects.requireNonNull(mapper);
            this.objectMapper = mapper;
            return this;
        }

        /**
         * Registers a {@link StatementAnalyzer}.
         * <p>
         * This handles individual statements.
         * Please attention the kind name of statements has the common suffix {@code _statement}.
         * </p>
         * <p>
         * This operation may replaces the default implementation of {@link StatementAnalyzer}.
         * </p>
         * @param kind the target statement kind
         * @param analyzer the statement analyzer, or {@code null} to unregister the default analyzer
         * @return this
         */
        public Builder register(@Nonnull String kind, @Nullable StatementAnalyzer analyzer) {
            Objects.requireNonNull(kind);
            LOG.trace("register: kind={}, analyzer={}", kind, analyzer); //$NON-NLS-1$
            statementAnalyzers.put(kind, analyzer);
            return this;
        }

        /**
         * Registers a {@link PropertyExtractor}.
         * <p>
         * This handles individual relational operators and exchange operators.
         * Please attention the kind name of exchange operators has the common suffix {@code _exchange}.
         * </p>
         * <p>
         * This operation may replaces the default implementation of {@link PropertyExtractor}.
         * </p>
         * @param kind the target relational operator or exchange kind
         * @param analyzer the property extractor, or {@code null} to unregister the default analyzer
         * @return this
         */
        public Builder register(@Nonnull String kind, @Nullable PropertyExtractor analyzer) {
            Objects.requireNonNull(kind);
            LOG.trace("register: kind={}, analyzer={}", kind, analyzer); //$NON-NLS-1$
            propertyExtractors.put(kind, analyzer);
            return this;
        }

        /**
         * Registers a {@link EdgeExtractor}.
         * <p>
         * This handles individual relation operators to extract connection to the exchange operators.
         * This is designed ONLY FOR EXPERT USERS - it may break out the execution plan graph structure.
         * </p>
         * <p>
         * This operation may replaces the default implementation of {@link EdgeExtractor}.
         * </p>
         * @param kind the target relational operator or exchange kind
         * @param analyzer the edge extractor, or {@code null} to unregister the default analyzer
         * @return this
         */
        public Builder register(@Nonnull String kind, @Nullable EdgeExtractor analyzer) {
            Objects.requireNonNull(kind);
            LOG.trace("register: kind={}, analyzer={}", kind, analyzer); //$NON-NLS-1$
            edgeExtractors.put(kind, analyzer);
            return this;
        }

        /**
         * Adds operator kind names to include in the graph.
         * @param kinds the operator kinds to keep in the graph
         * @return this
         * @see #withExcludeOperators(Collection)
         * @see #withNodeFilter(Predicate)
         */
        public Builder withIncludeOperators(@Nonnull Collection<String> kinds) {
            Objects.requireNonNull(kinds);
            LOG.trace("includes: {}", kinds); //$NON-NLS-1$
            this.includeOperatorKinds.addAll(kinds);
            return this;
        }

        /**
         * Adds operator kind names to exclude from the graph.
         * @param kinds the operator kinds to keep in the graph
         * @return this
         */
        public Builder withExcludeOperators(@Nonnull Collection<String> kinds) {
            Objects.requireNonNull(kinds);
            LOG.trace("excludes: {}", kinds); //$NON-NLS-1$
            this.excludeOperatorKinds.addAll(kinds);
            return this;
        }

        /**
         * Sets node filter to simplify the execution plan graph.
         * <p>
         * If it is not set, the execution plan graph will retain only important operators.
         * Note that, these also contain {@link #register(String, PropertyExtractor) analysis target operators}.
         * </p>
         * <p>
         * {@link JsonPlanGraphLoader} decides whether to keep individual operators as follows:
         * </p>
         * <ol>
         * <li>
         * First, each operator which kind is in {@link #withExcludeOperators(Collection) exclude list} is removed.
         * </li>
         * <li>
         * Otherwise, each operator which kind is in {@link #withIncludeOperators(Collection) include list} is retained.
         * </li>
         * <li>
         * Otherwise, each operator which {@link #withNodeFilter(Predicate) node filter} accepts is retained.
         * </li>
         * <li>
         * Otherwise, all operators are filtered out.
         * </li>
         * </ol>
         * @param filter the node filter, or {@code null} to use the default filter
         * @return this
         */
        public Builder withNodeFilter(@Nullable Predicate<? super PlanNode> filter) {
            LOG.trace("node filter: {}", filter); //$NON-NLS-1$
            this.nodeFilter = filter;
            return this;
        }

        /**
         * Builds a {@link JsonPlanGraphLoader}.
         * @return the built object
         */
        public JsonPlanGraphLoader build() {
            var properties = merge(DEFAULT_PROPERTY_EXTRACTORS, propertyExtractors);
            var edges = merge(DEFAULT_EDGE_EXTRACTORS, edgeExtractors);
            var filter = computeNodeFilter(properties);
            var config = new Config(properties, edges, filter);

            var defaultStatementAnalyzers = new HashMap<String, StatementAnalyzer>();
            DEFAULT_STATEMENT_ANALYZERS.forEach((k, v) -> defaultStatementAnalyzers.put(k, v.apply(config)));

            var statements = merge(defaultStatementAnalyzers, statementAnalyzers);

            LOG.trace("building JsonPlanGraphLoader: {}", statements); //$NON-NLS-1$
            return new JsonPlanGraphLoader(
                    objectMapper != null ? objectMapper : new ObjectMapper(),
                    statements);
        }

        private static <K, V> Map<K, V> merge(Map<K, V> defaults, Map<K, V> overwrites) {
            var results = new HashMap<>(defaults);
            overwrites.forEach((k, v) -> {
                if (v == null) {
                    results.remove(k);
                } else {
                    results.put(k, v);
                }
            });
            return results;
        }

        private Predicate<PlanNode> computeNodeFilter(Map<String, PropertyExtractor> properties) {
            var includes = Set.copyOf(includeOperatorKinds);
            var excludes = Set.copyOf(excludeOperatorKinds);
            Predicate<? super PlanNode> defaults =
                    nodeFilter != null ? nodeFilter : computeDefaultNodeFilter(properties);
            return node -> {
                if (excludes.contains(node.getKind())) {
                    return false;
                }
                if (includes.contains(node.getKind())) {
                    return true;
                }
                return defaults.test(node);
            };
        }

        private static Predicate<PlanNode> computeDefaultNodeFilter(Map<String, PropertyExtractor> properties) {
            var targets = Set.copyOf(properties.keySet());
            return node -> targets.contains(node.getKind());
        }
    }

    private static class Config {

        final StepGraphAnalyzer stepGraphAnalyzer;

        final Predicate<? super PlanNode> nodeFilter;

        Config(
                Map<String, PropertyExtractor> propertyExtractors,
                Map<String, EdgeExtractor> edgeExtractors,
                Predicate<? super PlanNode> nodeFilter) {
            this.stepGraphAnalyzer = new StepGraphAnalyzer(propertyExtractors, edgeExtractors, nodeFilter);
            this.nodeFilter = nodeFilter;
        }
    }

    private final ObjectMapper mapper;

    private final Map<String, StatementAnalyzer> analyzers;

    /**
     * Creates a new builder for {@link JsonPlanGraphLoader}.
     * @return the created builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a new instance.
     * @param mapper JSON parser
     * @param analyzers the enclosing statement analyzers
     * @see #newBuilder()
     */
    public JsonPlanGraphLoader(
            @Nonnull ObjectMapper mapper,
            @Nonnull Map<String, ? extends StatementAnalyzer> analyzers) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(analyzers);
        this.mapper = mapper;
        this.analyzers = Map.copyOf(analyzers);
    }

    @Override
    public boolean isSupported(String formatId, long formatVersion) {
        Objects.requireNonNull(formatId);
        return formatId.equals(SUPPORTED_FORMAT_ID)
                && SUPPORTED_FORMAT_VERSION_MIN <= formatVersion
                && formatVersion <= SUPPORTED_FORMAT_VERSION_MAX;
    }

    @Override
    public PlanGraph load(@Nonnull String text) throws PlanGraphException {
        Objects.requireNonNull(text);
        LOG.trace("loading JSON text: {}", text); //$NON-NLS-1$
        JsonNode node;
        try {
            node = mapper.readTree(text);
        } catch (JsonProcessingException e) {
            throw new PlanGraphException("explain information is malformed JSON text", e);
        }
        return analyzeStatement(node);
    }

    private PlanGraph analyzeStatement(JsonNode node) throws PlanGraphException {
        var kind = JsonUtil.getKind(node) + KIND_SUFFIX_STATEMENT;
        LOG.trace("analyze statement: kind={}", kind); //$NON-NLS-1$

        var analyzer = analyzers.get(kind);
        LOG.trace("using analyzer for statement: kind={}, extractor={}", kind, analyzer); //$NON-NLS-1$
        if (analyzer == null) {
            throw new PlanGraphException(MessageFormat.format(
                    "unsupported statement: kind={0}",
                    kind));
        }

        return analyzer.analyze((ObjectNode) node);
    }
}
