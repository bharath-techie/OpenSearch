/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchFilterExtractor;
import org.opensearch.analytics.exec.canmatch.LongRange;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.spi.FastPathHintSpec;
import org.opensearch.analytics.spi.FastPathHintSpec.BucketOp;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
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

    /** A recognised top-K fragment: per-RG candidate budget and truncation direction. */
    private record TopK(int budget, boolean keepLast) {
    }

    /** A recognised histogram fragment: the monotonic integer bucket op + operand. */
    private record Histogram(BucketOp op, long operand) {
    }

    /**
     * Computes the fast-path hints for a shard fragment.
     *
     * @param fragment                    the resolved (single-backend, annotations intact) shard fragment
     * @param leadingSortField            the leading {@code index.sort.field}, or {@code null} when the
     *                                    index has no sort — fails closed to {@link FastPathHintSpec#NONE}
     * @param leadingSortFieldMappingType the leading sort field's mapping type ({@code "date"},
     *                                    {@code "date_nanos"}, numeric types, …); drives the range unit
     * @param drivingBackend              the backend the fragment resolved to; a conjunct annotated for
     *                                    any other backend is a delegated (e.g. Lucene) leaf
     * @param indexSortDescending         true when {@code index.sort.order} is {@code desc} for the
     *                                    leading field; used to derive the TOPK keep-last direction
     */
    public static FastPathHintSpec extract(
        RelNode fragment,
        String leadingSortField,
        String leadingSortFieldMappingType,
        String drivingBackend,
        boolean indexSortDescending
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

        // Shapes are mutually exclusive: COUNT_ONLY (aggregate, no group key), TOPK (sort + head),
        // HISTOGRAM (aggregate grouped by monotonic int arithmetic). First match wins; otherwise a
        // range-only hint still enables the native timestamp-strip path.
        if (isCountOnly(fragment, filter, leadingSortField, drivingBackend)) {
            return FastPathHintSpec.countOnly(strippable, unit, lower, upper);
        }
        // Truncating candidates per RG is only sound if nothing filters rows after truncation:
        // every conjunct must be the (tautological on WITHIN RGs) sort range or a Lucene-delegated leaf.
        TopK topK = conjunctsAreRangeOrDelegated(filter, leadingSortField, drivingBackend)
            ? detectTopK(fragment, leadingSortField, indexSortDescending)
            : null;
        if (topK != null) {
            return FastPathHintSpec.topK(topK.budget(), topK.keepLast(), true, strippable, unit, lower, upper);
        }
        Histogram histogram = detectHistogram(fragment, filter, leadingSortField, drivingBackend, unit);
        if (histogram != null) {
            return FastPathHintSpec.histogram(histogram.op(), histogram.operand(), strippable, unit, lower, upper);
        }
        return FastPathHintSpec.rangeOnly(strippable, unit, lower, upper);
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
        return conjunctsAreRangeOrDelegated(filter, leadingSortField, drivingBackend);
    }

    /**
     * TOPK: {@code sort <leading> | head N}. The fragment spine from the Sort down to the Scan must
     * be truncation-safe — only identity Project/Filter/SubqueryAlias and exactly one Sort; a Window,
     * Aggregate, Join, set-op, Unnest, a second (inner) Sort/Limit, or a value-computing Projection
     * makes per-row-group candidate truncation unsound, so it fails closed. The single sort key must
     * be the leading index-sort field with a literal fetch. {@code keepLast} is true when the query
     * sort direction differs from the index sort order (newest N = LAST N of an index-sorted RG).
     */
    private static TopK detectTopK(RelNode fragment, String leadingSortField, boolean indexSortDescending) {
        Sort sort = null;
        RelNode node = unwrap(fragment);
        while (true) {
            node = unwrap(node);
            if (node.getInputs().isEmpty()) {
                if (node.getTable() == null) {
                    return null; // spine must bottom out at a table scan
                }
                break;
            }
            if (node instanceof Sort s) {
                if (sort != null) {
                    return null; // a second (inner) Sort/Limit reorders below the outer one — unsafe
                }
                sort = s;
            } else if (node instanceof Filter) {
                // identity filter preserves row identity and order within the RG — spine-safe
            } else if (node instanceof Project project) {
                if (isPureColumnProject(project) == false) {
                    return null; // a value-computing / sort-key-remapping Project is unsafe
                }
            } else {
                return null; // Aggregate/Window/Join/SetOp/Unnest/etc — not truncation-safe
            }
            if (node.getInputs().size() != 1) {
                return null;
            }
            node = node.getInputs().get(0);
        }
        if (sort == null) {
            return null;
        }
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        if (collations.size() != 1) {
            return null; // single sort key only — a secondary key changes which rows survive
        }
        int budget = resolveBudget(sort);
        if (budget <= 0) {
            return null; // needs a literal fetch (optionally + literal offset)
        }
        RelFieldCollation primary = collations.get(0);
        List<RelDataTypeField> fields = sort.getInput().getRowType().getFieldList();
        int idx = primary.getFieldIndex();
        if (idx < 0 || idx >= fields.size() || fields.get(idx).getName().equals(leadingSortField) == false) {
            return null;
        }
        boolean keepLast = primary.getDirection().isDescending() != indexSortDescending;
        return new TopK(budget, keepLast);
    }

    /** Coordinator rows to keep: {@code offset + fetch}, literal-only; {@code -1} when unavailable. */
    private static int resolveBudget(Sort sort) {
        if ((sort.fetch instanceof RexLiteral) == false) {
            return -1;
        }
        int fetch = RexLiteral.intValue(sort.fetch);
        if (fetch <= 0) {
            return -1;
        }
        if (sort.offset == null) {
            return fetch;
        }
        if ((sort.offset instanceof RexLiteral) == false) {
            return -1;
        }
        int offset = RexLiteral.intValue(sort.offset);
        if (offset < 0) {
            return -1;
        }
        long total = (long) offset + fetch;
        return total > Integer.MAX_VALUE ? -1 : (int) total;
    }

    /** True when every project expression is a bare column reference (permutation / passthrough). */
    private static boolean isPureColumnProject(Project project) {
        return project.getProjects().stream().allMatch(RexInputRef.class::isInstance);
    }

    /**
     * HISTOGRAM: Aggregate (partial or single-stage) with exactly one group key that is monotonic
     * integer arithmetic on the leading sort field ({@code col}, {@code col/N}, {@code col+N},
     * {@code col-N}, {@code col*N} with a positive int literal N) and exactly one non-distinct
     * {@code count(*)}/{@code count(literal)} with no per-agg FILTER, over Filter over Scan whose
     * conjuncts are only the sort-range + delegated leaves. Any other shape fails closed.
     *
     * <p>PPL {@code span()} lowering (see {@code SpanAdapter}) is matched by {@link #classifyBucket}:
     * {@code SpanAdapter.rewriteNumericSpan} emits the composite {@code (col/N)*N} (FLOOR-wrapped for
     * non-integer results, optionally CAST-wrapped) and the interval-1 time span emits
     * {@code date_trunc(second|minute|hour, col)} — both classified as {@link BucketOp#FLOOR_TO_MULTIPLE}.
     * The single-op forms ({@code col/N}, {@code col+N}, {@code col-N}, {@code col*N}) are also kept.
     */
    private static Histogram detectHistogram(
        RelNode fragment,
        Filter filter,
        String leadingSortField,
        String drivingBackend,
        FastPathHintSpec.RangeUnit unit
    ) {
        Aggregate agg = findAggregate(fragment);
        if (agg == null || agg.getGroupSets().size() != 1 || agg.getGroupSet().cardinality() != 1) {
            return null; // exactly one grouping set with exactly one group key
        }
        if (agg.getAggCallList().size() != 1) {
            return null;
        }
        AggregateCall call = agg.getAggCallList().get(0);
        if (call.getAggregation().getKind() != SqlKind.COUNT || call.isDistinct() || call.filterArg >= 0) {
            return null; // single non-distinct count(*), no per-agg FILTER
        }
        if (aggregateAtTop(fragment, agg) == false) {
            return null; // a HAVING/Sort/Window above the aggregate is not the histogram shape
        }
        if (aggregateOverFilterOverScan(agg) == false) {
            return null;
        }
        if (conjunctsAreRangeOrDelegated(filter, leadingSortField, drivingBackend) == false) {
            return null;
        }
        int groupIndex = agg.getGroupSet().nth(0);
        return matchBucketExpr(agg.getInput(), groupIndex, leadingSortField, unit);
    }

    /**
     * Resolves the group key's producing expression and classifies it as a monotonic integer bucket
     * on the leading sort field. When the aggregate input is a Project, the group key indexes into its
     * project list; otherwise the group key must be a bare leading-sort-field column (identity bucket).
     */
    private static Histogram matchBucketExpr(RelNode aggInput, int groupIndex, String leadingSortField, FastPathHintSpec.RangeUnit unit) {
        RelNode input = unwrap(aggInput);
        if (input instanceof Project project) {
            if (groupIndex < 0 || groupIndex >= project.getProjects().size()) {
                return null;
            }
            return classifyBucket(
                project.getProjects().get(groupIndex),
                project.getInput().getRowType().getFieldList(),
                leadingSortField,
                unit
            );
        }
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        if (groupIndex >= 0 && groupIndex < fields.size() && fields.get(groupIndex).getName().equals(leadingSortField)) {
            return new Histogram(BucketOp.NONE, 0L); // group by the raw sort column — identity bucket
        }
        return null;
    }

    /**
     * {@code col} =&gt; NONE; single-op {@code col op N} (op in /,+,-,*; N positive int literal); the
     * composite {@code (col/N)*N} (SpanAdapter numeric span, FLOOR/CAST wrapper optional) and
     * {@code date_trunc(second|minute|hour, col)} (SpanAdapter interval-1 time span) =&gt;
     * {@link BucketOp#FLOOR_TO_MULTIPLE}.
     */
    private static Histogram classifyBucket(
        RexNode expr,
        List<RelDataTypeField> fields,
        String leadingSortField,
        FastPathHintSpec.RangeUnit unit
    ) {
        RexNode node = stripCast(expr);
        if (node instanceof RexInputRef ref) {
            return refIsLeading(ref, fields, leadingSortField) ? new Histogram(BucketOp.NONE, 0L) : null;
        }
        if (!(node instanceof RexCall call)) {
            return null;
        }
        // SpanAdapter.rewriteNumericSpan: (col / N) * N (FLOOR-wrapped for non-integer results).
        Histogram floorMul = matchFloorToMultiple(call, fields, leadingSortField);
        if (floorMul != null) {
            return floorMul;
        }
        // SpanAdapter time span (interval == 1): DATE_TRUNC(unitLiteral, col).
        Histogram dateTrunc = matchDateTrunc(call, fields, leadingSortField, unit);
        if (dateTrunc != null) {
            return dateTrunc;
        }
        return matchSingleOp(call, fields, leadingSortField);
    }

    /** Single-op {@code col op N}: op in /,+,-,*; N a positive int literal; no column-side CAST. */
    private static Histogram matchSingleOp(RexCall call, List<RelDataTypeField> fields, String leadingSortField) {
        if (call.getOperands().size() != 2) {
            return null;
        }
        BucketOp op = switch (call.getKind()) {
            case DIVIDE -> BucketOp.DIV;
            case PLUS -> BucketOp.ADD;
            case MINUS -> BucketOp.SUB;
            case TIMES -> BucketOp.MUL;
            default -> null;
        };
        if (op == null) {
            return null;
        }
        // col must be the left operand (no column-side CAST); the right operand a positive int literal.
        if (!(call.getOperands().get(0) instanceof RexInputRef ref) || refIsLeading(ref, fields, leadingSortField) == false) {
            return null;
        }
        Long operand = positiveIntLiteral(call.getOperands().get(1));
        return operand == null ? null : new Histogram(op, operand);
    }

    /**
     * Matches {@code SpanAdapter.rewriteNumericSpan}'s composite {@code MULTIPLY((col / N), N)} — the
     * left factor is either the bare {@code DIVIDE(col, N)} (integer result) or {@code FLOOR(DIVIDE(col, N))}
     * (non-integer result); the same positive int literal {@code N} must appear as both divisor and
     * multiplier. Any outer CAST is peeled by {@link #classifyBucket}. Bucket size = {@code N}.
     */
    private static Histogram matchFloorToMultiple(RexCall call, List<RelDataTypeField> fields, String leadingSortField) {
        if (call.getKind() != SqlKind.TIMES || call.getOperands().size() != 2) {
            return null;
        }
        Long multiplier = positiveIntLiteral(call.getOperands().get(1));
        if (multiplier == null) {
            return null;
        }
        RexNode left = stripCast(call.getOperands().get(0));
        if (left instanceof RexCall floor && floor.getKind() == SqlKind.FLOOR && floor.getOperands().size() == 1) {
            left = stripCast(floor.getOperands().get(0)); // FLOOR(DIVIDE(col, N)) for non-integer span
        }
        if (!(left instanceof RexCall div) || div.getKind() != SqlKind.DIVIDE || div.getOperands().size() != 2) {
            return null;
        }
        if (!(div.getOperands().get(0) instanceof RexInputRef ref) || refIsLeading(ref, fields, leadingSortField) == false) {
            return null;
        }
        Long divisor = positiveIntLiteral(div.getOperands().get(1));
        if (divisor == null || divisor.longValue() != multiplier.longValue()) {
            return null; // N must be identical in both positions to be a floor-to-multiple bucket
        }
        return new Histogram(BucketOp.FLOOR_TO_MULTIPLE, multiplier);
    }

    /**
     * Matches {@code SpanAdapter}'s interval-1 time span lowering {@code DATE_TRUNC(unitLiteral, col)}
     * (unit literal first, field second). Only the fixed sub-day units second/minute/hour are sound
     * as floor-to-multiple buckets; day and coarser are calendar/timezone dependent and fail closed.
     * The operand is the unit's second-count expressed in the field's declared {@link FastPathHintSpec.RangeUnit}
     * (millis =&gt; x1000, nanos =&gt; x1e9); a non-temporal field fails closed.
     */
    private static Histogram matchDateTrunc(
        RexCall call,
        List<RelDataTypeField> fields,
        String leadingSortField,
        FastPathHintSpec.RangeUnit unit
    ) {
        if (call.getOperator().getName().equalsIgnoreCase("DATE_TRUNC") == false || call.getOperands().size() != 2) {
            return null;
        }
        if (!(call.getOperands().get(0) instanceof RexLiteral unitLit) || unitLit.getValue() == null) {
            return null;
        }
        if (!(call.getOperands().get(1) instanceof RexInputRef ref) || refIsLeading(ref, fields, leadingSortField) == false) {
            return null;
        }
        String truncUnit = unitLit.getValueAs(String.class);
        if (truncUnit == null) {
            return null;
        }
        long seconds = switch (truncUnit.toLowerCase(Locale.ROOT)) {
            case "second" -> 1L;
            case "minute" -> 60L;
            case "hour" -> 3600L;
            default -> 0L; // day and coarser: calendar/timezone dependent
        };
        if (seconds == 0L) {
            return null;
        }
        long operand = switch (unit) {
            case SECONDS -> seconds;
            case MILLIS -> seconds * 1_000L;
            case MICROS -> seconds * 1_000_000L;
            case NANOS -> seconds * 1_000_000_000L;
            default -> 0L; // NONE — not a temporal field, so date_trunc can't apply
        };
        return operand > 0L ? new Histogram(BucketOp.FLOOR_TO_MULTIPLE, operand) : null;
    }

    /** Peels top-level CAST wrappers (SpanAdapter pins its result type with {@code makeCast}). */
    private static RexNode stripCast(RexNode node) {
        RexNode current = node;
        while (current instanceof RexCall call && call.getKind() == SqlKind.CAST && call.getOperands().size() == 1) {
            current = call.getOperands().get(0);
        }
        return current;
    }

    private static boolean refIsLeading(RexInputRef ref, List<RelDataTypeField> fields, String leadingSortField) {
        return ref.getIndex() >= 0 && ref.getIndex() < fields.size() && fields.get(ref.getIndex()).getName().equals(leadingSortField);
    }

    /** A positive integer literal's value, or {@code null} when the node is not one. */
    private static Long positiveIntLiteral(RexNode node) {
        if (!(node instanceof RexLiteral lit) || lit.getValue() == null) {
            return null;
        }
        switch (lit.getType().getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                break;
            default:
                return null;
        }
        Object value = lit.getValue();
        if (!(value instanceof BigDecimal bd) || bd.scale() > 0 || bd.signum() <= 0) {
            return null;
        }
        try {
            return bd.longValueExact();
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /** Every top-level filter conjunct is the leading-sort range or a delegated (non-driving) leaf. */
    private static boolean conjunctsAreRangeOrDelegated(Filter filter, String leadingSortField, String drivingBackend) {
        List<RelDataTypeField> fields = filter.getInput().getRowType().getFieldList();
        for (RexNode conjunct : flattenAnd(filter.getCondition())) {
            if (isLeadingSortRange(conjunct, fields, leadingSortField) || isDelegatedLeaf(conjunct, drivingBackend)) {
                continue;
            }
            return false; // native residual — DataFusion would need to decode, so not a pure fast path
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

    /** True when only identity Project/SubqueryAlias sit above the aggregate (no HAVING/Sort/Window). */
    private static boolean aggregateAtTop(RelNode fragment, Aggregate agg) {
        RelNode node = unwrap(fragment);
        while (node != agg) {
            if (node instanceof Project project) {
                if (isPureColumnProject(project) == false) {
                    return false; // a value-computing Project reshapes the aggregate output
                }
            } else {
                return false; // a HAVING/Sort/Window between root and the aggregate breaks the shape
            }
            if (node.getInputs().size() != 1) {
                return false;
            }
            node = unwrap(node.getInputs().get(0));
        }
        return true;
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
