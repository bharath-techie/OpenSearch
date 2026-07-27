/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Expands grouped {@code COUNT(DISTINCT x)} into the exact two-level aggregate plan:
 *
 * <pre>
 *   Aggregate(group=[keys], dc=[COUNT(x)])          — count deduped values per key
 *     Aggregate(group=[keys ∪ {x}])                 — deduplicate (key, x) pairs
 *       input
 * </pre>
 *
 * <p><b>Why.</b> These used to be rewritten to {@code APPROX_COUNT_DISTINCT} (HLL sketches) by
 * {@link OpenSearchDistinctCountRule}. DataFusion's HLL accumulator keeps a dense 16&nbsp;KB
 * register array <em>per group</em>, so a high-group-count query like ClickBench q14
 * ({@code stats dc(UserID) by SearchPhrase}, ~6M distinct SearchPhrases) needs ~96&nbsp;GB of
 * sketch state and OOM-kills the node — while this exact expansion (the same plan DataFusion's
 * own optimizer produces for SQL {@code COUNT(DISTINCT)}) completes it in ~3.5&nbsp;GB. It is
 * also exact, matching PPL's documentation of {@code dc()} as a distinct count, where the HLL
 * path was silently ±0.8%.
 *
 * <p><b>Why the narrow gate.</b> Only aggregates where <em>every</em> call is a single-arg
 * {@code COUNT(DISTINCT x)} on the <em>same</em> argument, with at least one group key, are
 * expanded:
 * <ul>
 *   <li><b>Ungrouped</b> {@code dc(x)} keeps the HLL rewrite — there is exactly one group, so
 *       sketch state is one 16&nbsp;KB array, and the alternative ships every distinct value to
 *       the coordinator (ClickBench q05: 17.6M UserIDs vs one sketch).</li>
 *   <li><b>Mixed</b> ({@code stats count(), dc(x)}) or multi-distinct ({@code dc(a), dc(b)})
 *       shapes require grouping-set or join expansions (see Calcite's
 *       {@code AggregateExpandDistinctAggregatesRule}) that the fragment convertor does not
 *       support; they keep the HLL rewrite.</li>
 * </ul>
 *
 * <p><b>Distribution falls out of existing machinery.</b> Both emitted aggregates are ordinary
 * distinct-free aggregates. On a multi-shard plan, {@link OpenSearchAggregateSplitRule} splits
 * the bottom dedup aggregate into PARTIAL (per shard) / FINAL (coordinator) — deduplication is
 * idempotent under merge, so shards ship deduped {@code (key, x)} pairs rather than 16&nbsp;KB
 * sketches per key (for q14's shape that is also strictly less wire volume). The top count then
 * runs on the coordinator over the deduped rows. This mirrors how datafusion-distributed stages
 * the same query: partial dedup → shuffle on the group key → final dedup + count.
 *
 * <p>Runs in the {@code aggregate-decompose} HEP phase on plain {@link LogicalAggregate},
 * before marking, alongside {@link OpenSearchDistinctCountRule} — the two rules' gates are
 * disjoint by construction ({@code OpenSearchDistinctCountRule} defers to
 * {@link #expandsExactly(LogicalAggregate)}).
 *
 * @opensearch.internal
 */
public class OpenSearchExpandDistinctCountRule extends RelOptRule {

    public OpenSearchExpandDistinctCountRule() {
        super(operand(LogicalAggregate.class, any()), "OpenSearchExpandDistinctCountRule");
    }

    /**
     * True when this rule will expand the aggregate to the exact two-level plan: at least one
     * group key, at least one call, and every call is a single-arg non-filtered
     * {@code COUNT(DISTINCT x)} over the same {@code x}. Also consulted by
     * {@link OpenSearchDistinctCountRule} so the HLL rewrite never races this expansion.
     */
    static boolean expandsExactly(LogicalAggregate agg) {
        if (agg.getGroupSet().isEmpty() || agg.getAggCallList().isEmpty()) {
            return false;
        }
        if (agg.getGroupSets().size() > 1) {
            return false; // grouping sets: out of scope
        }
        int arg = -1;
        for (AggregateCall call : agg.getAggCallList()) {
            boolean simpleCountDistinct = call.getAggregation().getKind() == SqlKind.COUNT
                && call.isDistinct()
                && call.getArgList().size() == 1
                && call.filterArg < 0
                && call.rexList.isEmpty();
            if (!simpleCountDistinct) {
                return false;
            }
            int a = call.getArgList().get(0);
            if (arg == -1) {
                arg = a;
            } else if (arg != a) {
                return false; // dc(a), dc(b): needs a join expansion
            }
        }
        // A distinct arg that is also a group key means COUNT(DISTINCT key) — degenerate
        // (always 0 or 1 per group); leave it to the fallback path rather than special-case.
        return !agg.getGroupSet().get(arg);
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        return expandsExactly(ruleCall.rel(0));
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        int distinctArg = agg.getAggCallList().get(0).getArgList().get(0);

        // ── Bottom: dedup (keys ∪ {x}), no aggregate calls ──
        ImmutableBitSet bottomGroup = agg.getGroupSet().union(ImmutableBitSet.of(distinctArg));
        LogicalAggregate dedup = LogicalAggregate.create(
            agg.getInput(),
            agg.getHints(),
            bottomGroup,
            com.google.common.collect.ImmutableList.of(bottomGroup),
            List.of()
        );

        // ── Top: original keys, COUNT(x) — plain, x is already distinct per key ──
        // Bottom output columns are bottomGroup's bits in ascending order; remap indices.
        List<Integer> bottomCols = bottomGroup.asList();
        ImmutableBitSet topGroup = ImmutableBitSet.of(
            agg.getGroupSet().asList().stream().map(bottomCols::indexOf).toList()
        );
        int xInBottom = bottomCols.indexOf(distinctArg);

        List<AggregateCall> topCalls = new ArrayList<>(agg.getAggCallList().size());
        for (AggregateCall call : agg.getAggCallList()) {
            topCalls.add(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    /* distinct */ false,
                    /* approximate */ false,
                    call.ignoreNulls(),
                    call.rexList,
                    List.of(xInBottom),
                    /* filterArg */ -1,
                    /* distinctKeys */ null,
                    call.collation,
                    topGroup.cardinality(),
                    dedup,
                    /* type */ call.getType(),
                    call.getName()
                )
            );
        }
        LogicalAggregate top = LogicalAggregate.create(
            dedup,
            agg.getHints(),
            topGroup,
            com.google.common.collect.ImmutableList.of(topGroup),
            topCalls
        );

        ruleCall.transformTo(projectToOriginalRowType(ruleCall, agg, top));
    }

    /**
     * The top aggregate emits group keys in {@code bottomGroup}-relative ascending order, which
     * matches the original's key order (both ascend over the same key set), but HepPlanner
     * requires exact row-type equality — bridge names/nullability with a Project when needed.
     */
    private static RelNode projectToOriginalRowType(RelOptRuleCall ruleCall, LogicalAggregate original, LogicalAggregate replacement) {
        if (replacement.getRowType().equals(original.getRowType())) {
            return replacement;
        }
        RelBuilder relBuilder = ruleCall.builder();
        relBuilder.push(replacement);
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RelDataTypeField> origFields = original.getRowType().getFieldList();
        List<RelDataTypeField> newFields = replacement.getRowType().getFieldList();
        List<RexNode> projects = new ArrayList<>(origFields.size());
        List<String> names = new ArrayList<>(origFields.size());
        for (int i = 0; i < origFields.size(); i++) {
            RexNode ref = rexBuilder.makeInputRef(replacement, i);
            if (!newFields.get(i).getType().equals(origFields.get(i).getType())) {
                ref = rexBuilder.makeCast(origFields.get(i).getType(), ref);
            }
            projects.add(ref);
            names.add(origFields.get(i).getName());
        }
        relBuilder.project(projects, names, /* forceProject */ true);
        return relBuilder.build();
    }
}
