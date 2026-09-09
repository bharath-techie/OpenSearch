/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchFilterExtractor;
import org.opensearch.analytics.exec.canmatch.LongRange;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.spi.FastPathHintSpec;

import java.util.List;
import java.util.Set;

/**
 * Decides a shard fragment's fast-path shape ONCE at conversion time and emits a
 * {@link FastPathHintSpec}. This is the ONLY place plan shape is decided — the native executor
 * makes a single per-row-group decision from footer stats + these hints + {@code has_deletes},
 * with no plan sniffing in Rust.
 *
 * <p>Reuses {@link CanMatchFilterExtractor} for the range bounds (it already const-folds
 * {@code TIMESTAMP('...')} / {@code CAST} literals and refuses top-level OR). Every rule fails
 * closed to {@link FastPathHintSpec#NONE} / a flag off.
 *
 * @opensearch.internal
 */
public final class FastPathHintExtractor {

    /** Range comparison / collapsed-range operators whose bounds live on the leading sort field. */
    private static final Set<SqlKind> RANGE_KINDS = Set.of(
        SqlKind.GREATER_THAN,
        SqlKind.GREATER_THAN_OR_EQUAL,
        SqlKind.LESS_THAN,
        SqlKind.LESS_THAN_OR_EQUAL,
        SqlKind.BETWEEN,
        SqlKind.SEARCH
    );

    private FastPathHintExtractor() {}

    /**
     * Computes the fast-path hints for a shard fragment.
     *
     * @param fragment                 the resolved (single-backend, annotations intact) shard fragment
     * @param leadingSortField         the leading {@code index.sort.field}, or {@code null} when the
     *                                 index has no sort — fails closed to {@link FastPathHintSpec#NONE}
     * @param leadingSortFieldMappingType the leading sort field's mapping type ({@code "date"},
     *                                 {@code "date_nanos"}, numeric types, …); drives the range unit
     * @param drivingBackend           the backend the fragment resolved to; a conjunct annotated for
     *                                 any other backend is a delegated (e.g. Lucene) leaf
     */
    public static FastPathHintSpec extract(
        RelNode fragment,
        String leadingSortField,
        String leadingSortFieldMappingType,
        String drivingBackend
    ) {
        if (fragment == null || leadingSortField == null || leadingSortField.isBlank()) {
            return FastPathHintSpec.NONE;
        }
        Filter filter = findFilter(fragment);
        if (filter == null) {
            return FastPathHintSpec.NONE;
        }

        // Range bounds on the leading sort field. Intersect the (possibly two) conjuncts that bound
        // it; decline if any extracted range targets a different column (that would be a residual the
        // footer-of-sort-column check can't cover).
        long lower = Long.MIN_VALUE;
        long upper = Long.MAX_VALUE;
        int leadingRangeCount = 0;
        for (CanMatchFilter cmf : CanMatchFilterExtractor.extract(fragment)) {
            if (!(cmf instanceof LongRange range)) {
                return FastPathHintSpec.NONE;
            }
            if (range.column().equals(leadingSortField)) {
                lower = Math.max(lower, range.min());
                upper = Math.min(upper, range.max());
                leadingRangeCount++;
            } else {
                return FastPathHintSpec.NONE;
            }
        }
        if (leadingRangeCount == 0) {
            // No literal range on the leading sort field — nothing the footer stats can gate on.
            return FastPathHintSpec.NONE;
        }

        boolean strippable = isPureConjunction(filter.getCondition());
        FastPathHintSpec.RangeUnit unit = unitOf(leadingSortFieldMappingType);
        FastPathHintSpec.Shape shape = isCountOnly(fragment, filter, leadingSortField, drivingBackend)
            ? FastPathHintSpec.Shape.COUNT_ONLY
            : FastPathHintSpec.Shape.NONE;

        return new FastPathHintSpec(shape, true, strippable, unit, lower, upper);
    }

    /** date =&gt; millis, date_nanos =&gt; nanos; everything else carries no temporal unit. */
    private static FastPathHintSpec.RangeUnit unitOf(String mappingType) {
        if ("date_nanos".equals(mappingType)) {
            return FastPathHintSpec.RangeUnit.NANOS;
        }
        if ("date".equals(mappingType)) {
            return FastPathHintSpec.RangeUnit.MILLIS;
        }
        return FastPathHintSpec.RangeUnit.NONE;
    }

    /**
     * COUNT_ONLY: fragment is an Aggregate with a single non-distinct {@code count(*)}/
     * {@code count(literal)} and NO group keys, over Filter over Scan, where every top-level filter
     * conjunct is either the leading-sort range or a delegated (non-driving-backend) leaf. Any group
     * key, extra agg, distinct, or native residual predicate fails closed.
     */
    private static boolean isCountOnly(RelNode fragment, Filter filter, String leadingSortField, String drivingBackend) {
        Aggregate agg = findAggregate(fragment);
        if (agg == null || agg.getGroupCount() != 0 || agg.getAggCallList().size() != 1) {
            return false;
        }
        AggregateCall call = agg.getAggCallList().get(0);
        if (call.getAggregation().getKind() != SqlKind.COUNT || call.isDistinct()) {
            return false;
        }
        if (aggregateOverFilterOverScan(agg) == false) {
            return false;
        }
        List<RelDataTypeField> fields = filter.getInput().getRowType().getFieldList();
        for (RexNode conjunct : flattenAnd(filter.getCondition())) {
            if (isLeadingSortRange(conjunct, fields, leadingSortField)) {
                continue;
            }
            if (isDelegatedLeaf(conjunct, drivingBackend)) {
                continue;
            }
            return false; // native residual — DataFusion would need to decode, so not count-only
        }
        return true;
    }

    /** True when the spine from the aggregate down to a TableScan contains only Filter/Project. */
    private static boolean aggregateOverFilterOverScan(Aggregate agg) {
        RelNode node = agg.getInput();
        while (node != null) {
            node = unwrap(node);
            if (node.getInputs().isEmpty()) {
                return node.getTable() != null; // reached a leaf: must be a table scan
            }
            if (node instanceof Sort || node instanceof Window || node instanceof Join || node instanceof Aggregate) {
                return false;
            }
            if (node.getInputs().size() != 1) {
                return false;
            }
            node = node.getInputs().get(0);
        }
        return false;
    }

    private static boolean isLeadingSortRange(RexNode conjunct, List<RelDataTypeField> fields, String leadingSortField) {
        if (!(conjunct instanceof RexCall call) || RANGE_KINDS.contains(call.getKind()) == false) {
            return false;
        }
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef ref
                && ref.getIndex() < fields.size()
                && fields.get(ref.getIndex()).getName().equals(leadingSortField)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDelegatedLeaf(RexNode conjunct, String drivingBackend) {
        return conjunct instanceof AnnotatedPredicate ap
            && ap.getViableBackends().isEmpty() == false
            && ap.getViableBackends().get(0).equals(drivingBackend) == false;
    }

    /**
     * The condition is a pure conjunction (Fix 10): no OR/NOT in the top-level boolean spine, so the
     * leading-sort range conjunct can be stripped leaving a pure AND of the other conjuncts. Delegated
     * predicates are opaque leaves — their internal structure doesn't affect strippability.
     */
    private static boolean isPureConjunction(RexNode node) {
        if (node instanceof AnnotatedPredicate) {
            return true;
        }
        if (!(node instanceof RexCall call)) {
            return true;
        }
        return switch (call.getKind()) {
            case AND -> call.getOperands().stream().allMatch(FastPathHintExtractor::isPureConjunction);
            case OR, NOT -> false;
            default -> true;
        };
    }

    private static List<RexNode> flattenAnd(RexNode condition) {
        if (condition instanceof RexCall call && call.getKind() == SqlKind.AND) {
            return call.getOperands();
        }
        return List.of(condition);
    }

    private static Filter findFilter(RelNode node) {
        RelNode current = unwrap(node);
        if (current instanceof Filter filter) {
            return filter;
        }
        for (RelNode input : current.getInputs()) {
            Filter found = findFilter(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Aggregate findAggregate(RelNode node) {
        RelNode current = unwrap(node);
        if (current instanceof Aggregate aggregate) {
            return aggregate;
        }
        for (RelNode input : current.getInputs()) {
            Aggregate found = findAggregate(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Peels a Calcite HepRelVertex/RelSubset wrapper so {@code instanceof} checks see the real node. */
    private static RelNode unwrap(RelNode node) {
        return org.opensearch.analytics.planner.RelNodeUtils.unwrapHep(node);
    }
}
