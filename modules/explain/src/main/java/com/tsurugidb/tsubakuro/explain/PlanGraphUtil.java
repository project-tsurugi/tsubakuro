package com.tsurugidb.tsubakuro.explain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities about {@link PlanGraph}.
 */
public final class PlanGraphUtil {

    static final Logger LOG = LoggerFactory.getLogger(PlanGraphUtil.class);

    private static class BasicBlock {
        final List<PlanNode> elements = new ArrayList<>();
        final List<BasicBlock> successors = new ArrayList<>();
        final List<BasicBlock> predecessors = new ArrayList<>();
        @Override
        public String toString() {
            if (elements.isEmpty()) {
                return "BasicBlock()";
            }
            if (elements.size() == 1) {
                return String.format("BasicBlock(%s)", elements.get(0));
            }
            return String.format("BasicBlock(%s..%s)", elements.get(0), elements.get(elements.size() - 1));
        }
    }

    /**
     * Sorts individual plan nodes from upstreams to downstreams.
     * @param nodes the nodes to sort
     * @return the sorted list
     * @throws IllegalArgumentException if the nodes have cycle
     */
    public static List<PlanNode> sort(@Nonnull Collection<? extends PlanNode> nodes) {
        Objects.requireNonNull(nodes);
        var blocks = toBasicBlockGraph(nodes);
        assert blocks.stream().flatMap(it -> it.elements.stream()).count() == nodes.size();

        var sorted = rankSortBlocks(blocks);
        assert sorted.size() == blocks.size();

        var results = new ArrayList<PlanNode>(nodes.size());
        for (var block : sorted) {
            results.addAll(block.elements);
        }
        assert nodes.size() == results.size();
        return results;
    }

    private static List<BasicBlock> toBasicBlockGraph(@Nonnull Collection<? extends PlanNode> nodes) {
        var saw = new HashSet<PlanNode>();
        var heads = new ArrayDeque<PlanNode>();
        nodes.stream()
            .filter(it -> it.getUpstreams().isEmpty()) // collect sources
            .forEach(heads::addLast);

        // extract blocks
        var blocks = new ArrayList<BasicBlock>();
        while (!heads.isEmpty()) {
            var current = heads.removeFirst();
            if (saw.contains(current)) {
                continue;
            }
            saw.add(current);
            var block = new BasicBlock();
            while (true) {
                // add current node to current block
                block.elements.add(current);
                // check if continue block ..
                var nextSet = current.getDownstreams();
                if (nextSet.size() == 1) {
                    var next = nextSet.iterator().next();
                    if (next.getUpstreams().size() == 1) {
                        // continue block
                        current = next;
                        if (saw.contains(current)) {
                            throw new IllegalArgumentException("cycle detected in basic block");
                        }
                        saw.add(current);
                        continue;
                    }
                }
                // .. or split to other blocks
                for (var next : nextSet) {
                    heads.addLast(next);
                }
                break;
            }
            blocks.add(block);
        }

        if (saw.size() != nodes.size()) {
            throw new IllegalArgumentException("detects cycles in nodes");
        }

        // reconnect blocks
        var blockMap = new HashMap<PlanNode, BasicBlock>();
        for (var block : blocks) {
            blockMap.put(block.elements.get(0), block);
        }
        for (var block : blocks) {
            var tail = block.elements.get(block.elements.size() - 1);
            for (var nextNode : tail.getDownstreams()) {
                var nextBlock = blockMap.get(nextNode);
                assert nextBlock != null;
                block.successors.add(nextBlock);
                nextBlock.predecessors.add(block);
            }
        }

        return blocks;
    }

    private static List<BasicBlock> rankSortBlocks(List<BasicBlock> blocks) {
        var sorted = topologicalSort(blocks);
        assert sorted.size() == blocks.size();
        var rankMap = new HashMap<BasicBlock, Integer>();
        for (var block : sorted) {
            int max = -1;
            for (var pred : block.predecessors) {
                var rank = rankMap.get(pred);
                if (rank == null) {
                    throw new IllegalArgumentException("detect cycle in nodes");
                }
                max = Math.max(max, rank);
            }
            rankMap.put(block, max + 1);
        }
        Collections.sort(sorted, (a, b) -> {
            var aRank = rankMap.get(a);
            var bRank = rankMap.get(b);
            return Integer.compare(aRank, bRank);
        });
        return sorted;
    }

    private static List<BasicBlock> topologicalSort(List<BasicBlock> blocks) {
        var results = new ArrayList<BasicBlock>();
        var registered = new HashSet<BasicBlock>();
        var stack = new ArrayDeque<SortFrame>();
        for (var block : blocks) {
            if (registered.contains(block)) {
                continue;
            }
            LOG.trace("topological sort start: {}", block); //$NON-NLS-1$
            assert stack.isEmpty();
            stack.addLast(new SortFrame(block));
            registered.add(block);
            ENTER: while (!stack.isEmpty()) {
                var frame = stack.getLast();
                LOG.trace("topological sort enter frame: {}", frame.node); //$NON-NLS-1$
                while (frame.iterator.hasNext()) {
                    var next = frame.iterator.next();
                    if (!registered.contains(next)) {
                        stack.addLast(new SortFrame(next));
                        registered.add(next);
                        continue ENTER;
                    }
                }
                LOG.trace("topological sort exit frame: {}", frame.node); //$NON-NLS-1$
                stack.removeLast();
                results.add(frame.node);
            }
        }
        return results;
    }

    private static class SortFrame {

        final BasicBlock node;

        final Iterator<BasicBlock> iterator;

        SortFrame(BasicBlock node) {
            this.node = node;
            this.iterator = node.predecessors.iterator();
        }
    }

    private PlanGraphUtil() {
        throw new AssertionError();
    }
}
