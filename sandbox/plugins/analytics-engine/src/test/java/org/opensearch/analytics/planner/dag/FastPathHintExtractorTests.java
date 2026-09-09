/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.spi.FastPathHintSpec;

import java.util.List;

/**
 * Unit tests for {@link FastPathHintExtractor}. Fragments are assembled directly from Calcite rel
 * nodes (generic {@code Aggregate}/{@code Filter}, which the OpenSearch subclasses extend), so the
 * rules are exercised without running the whole planner.
 */
public class FastPathHintExtractorTests extends BasePlannerRulesTests {

    private static final String BACKEND = "datafusion";

    /** Scan with a leading sort field {@code ts} (BIGINT) and a {@code host} (VARCHAR). */
    private RelNode logsScan() {
        return stubScan(mockTable("logs", new String[] { "ts", "host" }, new SqlTypeName[] { SqlTypeName.BIGINT, SqlTypeName.VARCHAR }));
    }

    private RexNode tsAtLeast(RelNode input, long value) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(input.getRowType().getFieldList().get(0).getType(), 0),
            rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
    }

    private RexNode hostEquals(RelNode input, String value) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(input.getRowType().getFieldList().get(1).getType(), 1),
            rexBuilder.makeLiteral(value)
        );
    }

    private RelNode countOver(RelNode input) {
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(), null, List.of(count));
    }

    public void testCountOnlyHappyPath() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RelNode agg = countOver(filter);

        FastPathHintSpec hints = FastPathHintExtractor.extract(agg, "ts", "date", BACKEND, false);

        assertEquals(FastPathHintSpec.Shape.COUNT_ONLY, hints.shape());
        assertTrue(hints.rangeOnLeadingSortField());
        assertTrue(hints.rangeConjunctStrippable());
        assertEquals(FastPathHintSpec.RangeUnit.MILLIS, hints.rangeUnit());
        assertEquals(1000L, hints.rangeLowerInclusive());
        assertEquals(Long.MAX_VALUE, hints.rangeUpperInclusive());
    }

    public void testNoneForGroupKeys() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            filter,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        // group by host — a grouped aggregate is not a whole-fragment count.
        RelNode agg = LogicalAggregate.create(filter, List.of(), ImmutableBitSet.of(1), null, List.of(count));

        FastPathHintSpec hints = FastPathHintExtractor.extract(agg, "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
    }

    public void testNoneForRangeUnderOr() {
        RelNode scan = logsScan();
        RexNode or = rexBuilder.makeCall(SqlStdOperatorTable.OR, tsAtLeast(scan, 1000L), hostEquals(scan, "x"));
        RelNode filter = LogicalFilter.create(scan, or);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
        assertFalse("a range under a top-level OR must not be reported on the leading sort field", hints.rangeOnLeadingSortField());
    }

    public void testNoneForCastOnColumn() {
        RelNode scan = logsScan();
        RelDataType tsType = scan.getRowType().getFieldList().get(0).getType();
        // CAST(ts AS TIMESTAMP) > 1000 — the column side is a function, not a bare ref.
        RexNode castCol = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), rexBuilder.makeInputRef(tsType, 0));
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            castCol,
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RelNode filter = LogicalFilter.create(scan, cond);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
        assertFalse(hints.rangeOnLeadingSortField());
    }

    /** A pure AND with a native residual: the range is strippable, but the shape is not count-only. */
    public void testStrippableWithNativeResidual() {
        RelNode scan = logsScan();
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsAtLeast(scan, 1000L), hostEquals(scan, "foo"));
        RelNode filter = LogicalFilter.create(scan, and);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND, false);
        assertTrue(hints.rangeOnLeadingSortField());
        assertTrue("a pure conjunction is strippable", hints.rangeConjunctStrippable());
        assertEquals("a native residual predicate rules out count-only", FastPathHintSpec.Shape.NONE, hints.shape());
    }

    /** An OR sibling makes the residual non-pure-AND, so the range conjunct is not strippable. */
    public void testNotStrippableUnderOrSibling() {
        RelNode scan = logsScan();
        RexNode orSibling = rexBuilder.makeCall(SqlStdOperatorTable.OR, hostEquals(scan, "a"), hostEquals(scan, "b"));
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsAtLeast(scan, 1000L), orSibling);
        RelNode filter = LogicalFilter.create(scan, and);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND, false);
        assertTrue("the leading-sort range is still a top-level MUST conjunct", hints.rangeOnLeadingSortField());
        assertFalse("an OR sibling makes the residual non-pure-AND", hints.rangeConjunctStrippable());
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
    }

    public void testDateVersusDateNanosUnit() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RelNode agg = countOver(filter);

        assertEquals(FastPathHintSpec.RangeUnit.MILLIS, FastPathHintExtractor.extract(agg, "ts", "date", BACKEND, false).rangeUnit());
        assertEquals(FastPathHintSpec.RangeUnit.NANOS, FastPathHintExtractor.extract(agg, "ts", "date_nanos", BACKEND, false).rangeUnit());
    }

    /** A delegated (Lucene) leaf alongside the sort-range is allowed by count-only. */
    public void testCountOnlyAllowsDelegatedLeaf() {
        RelNode scan = logsScan();
        RexNode hostEq = hostEquals(scan, "foo");
        AnnotatedPredicate delegated = new AnnotatedPredicate(hostEq.getType(), hostEq, List.of("lucene"), 0);
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsAtLeast(scan, 1000L), delegated);
        RelNode filter = LogicalFilter.create(scan, and);
        RelNode agg = countOver(filter);

        FastPathHintSpec hints = FastPathHintExtractor.extract(agg, "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.COUNT_ONLY, hints.shape());
    }

    public void testNullLeadingSortFieldFailsClosed() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        assertEquals(FastPathHintSpec.NONE, FastPathHintExtractor.extract(filter, null, "date", BACKEND, false));
    }

    // ---- TOPK ----

    private RelNode descSort(RelNode input, int fetch) {
        RelCollation desc = RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));
        return LogicalSort.create(input, desc, null, rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true));
    }

    /** {@code where ts >= 1000 | sort ts | head 5} on an ASC index: TOPK, keep-first, budget 5. */
    public void testTopKHappyPathAscKeepsFirst() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RelNode sort = makeSort(filter, 5); // ASC on field 0 (ts)

        FastPathHintSpec hints = FastPathHintExtractor.extract(sort, "ts", "date", BACKEND, false);

        assertEquals(FastPathHintSpec.Shape.TOPK, hints.shape());
        assertEquals(5, hints.topKBudget());
        assertTrue(hints.topKPathSafe());
        assertFalse("ASC query on ASC index keeps the FIRST N", hints.topKKeepLast());
    }

    /** A native residual under the Sort would filter survivors after truncation, so TOPK is declined. */
    public void testTopKDeclinedWithNativeResidual() {
        RelNode scan = logsScan();
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsAtLeast(scan, 1000L), hostEquals(scan, "foo"));
        RelNode sort = makeSort(LogicalFilter.create(scan, and), 5);

        FastPathHintSpec hints = FastPathHintExtractor.extract(sort, "ts", "date", BACKEND, false);

        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
        assertTrue("the range-only hint still enables the timestamp-strip path", hints.rangeConjunctStrippable());
    }

    /** A DESC query on an ASC index keeps the LAST N of each index-sorted row group. */
    public void testTopKKeepLastForDescQueryOnAscIndex() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RelNode sort = descSort(filter, 10);

        FastPathHintSpec ascIndex = FastPathHintExtractor.extract(sort, "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.TOPK, ascIndex.shape());
        assertTrue("DESC query on ASC index keeps the LAST N", ascIndex.topKKeepLast());

        // Same DESC query on a DESC index keeps the FIRST N (directions agree).
        FastPathHintSpec descIndex = FastPathHintExtractor.extract(sort, "ts", "date", BACKEND, true);
        assertFalse(descIndex.topKKeepLast());
    }

    /** A value-computing Project between Sort and Scan makes truncation unsound — fail closed. */
    public void testNoneWhenComputeProjectBetweenSortAndScan() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RexNode tsPlus = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0),
            rexBuilder.makeLiteral(1L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RelNode project = LogicalProject.create(
            filter,
            List.of(),
            List.of(tsPlus, rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(1).getType(), 1)),
            (List<String>) null
        );
        RelNode sort = makeSort(project, 5);

        FastPathHintSpec hints = FastPathHintExtractor.extract(sort, "ts", "date", BACKEND, false);
        assertEquals("a non-identity Project between Sort and Scan is not truncation-safe", FastPathHintSpec.Shape.NONE, hints.shape());
    }

    // ---- HISTOGRAM ----

    /** {@code bucket = ts / 3600000} project + count group-by: HISTOGRAM with DIV / operand. */
    private RelNode histogramFragment(long divisor, boolean secondGroupKey) {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RexNode bucket = rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE,
            rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0),
            rexBuilder.makeLiteral(divisor, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RelNode project = LogicalProject.create(
            filter,
            List.of(),
            List.of(
                rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0),
                rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(1).getType(), 1),
                bucket
            ),
            (List<String>) null
        );
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            project,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        ImmutableBitSet groupSet = secondGroupKey ? ImmutableBitSet.of(2, 1) : ImmutableBitSet.of(2);
        return LogicalAggregate.create(project, List.of(), groupSet, null, List.of(count));
    }

    public void testHistogramHappyPathForColDivN() {
        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragment(3600000L, false), "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.HISTOGRAM, hints.shape());
        assertEquals(FastPathHintSpec.BucketOp.DIV, hints.histogramBucketOp());
        assertEquals(3600000L, hints.histogramBucketOperand());
    }

    public void testNoneWhenSecondGroupKey() {
        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragment(3600000L, true), "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
    }

    // ---- HISTOGRAM: PPL span() lowering (SpanAdapter) ----

    private RexNode tsRefBigint() {
        return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
    }

    private RexNode bigintLit(long value) {
        return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BIGINT), true);
    }

    /** Build {@code count(*) group by <bucketExpr>} over {@code filter(ts>=1000)} over the logs scan. */
    private RelNode histogramFragmentWithBucket(RexNode bucketExpr) {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RelNode project = LogicalProject.create(
            filter,
            List.of(),
            List.of(
                rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0),
                rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(1).getType(), 1),
                bucketExpr
            ),
            (List<String>) null
        );
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            project,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(2), null, List.of(count));
    }

    /** {@code (ts / N) * N} — SpanAdapter.rewriteNumericSpan integer-result form => FLOOR_TO_MULTIPLE. */
    public void testHistogramColDivNTimesN() {
        RexNode div = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, tsRefBigint(), bigintLit(1000L));
        RexNode mul = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, div, bigintLit(1000L));

        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragmentWithBucket(mul), "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.HISTOGRAM, hints.shape());
        assertEquals(FastPathHintSpec.BucketOp.FLOOR_TO_MULTIPLE, hints.histogramBucketOp());
        assertEquals(1000L, hints.histogramBucketOperand());
    }

    /** {@code FLOOR(ts / N) * N} — SpanAdapter.rewriteNumericSpan non-integer form => FLOOR_TO_MULTIPLE. */
    public void testHistogramFloorColDivNTimesN() {
        RexNode div = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, tsRefBigint(), bigintLit(500L));
        RexNode floor = rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, div);
        RexNode mul = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, floor, bigintLit(500L));

        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragmentWithBucket(mul), "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.HISTOGRAM, hints.shape());
        assertEquals(FastPathHintSpec.BucketOp.FLOOR_TO_MULTIPLE, hints.histogramBucketOp());
        assertEquals(500L, hints.histogramBucketOperand());
    }

    /** {@code (ts / A) * B} with A != B is not a floor-to-multiple bucket => NONE. */
    public void testNoneWhenDivMulLiteralsMismatch() {
        RexNode div = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, tsRefBigint(), bigintLit(1000L));
        RexNode mul = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, div, bigintLit(2000L));

        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragmentWithBucket(mul), "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
    }

    /** {@code date_trunc('hour', ts)} on a {@code date} field => FLOOR_TO_MULTIPLE, operand 3_600_000 ms. */
    public void testHistogramDateTruncHourOnDate() {
        RexNode dateTrunc = rexBuilder.makeCall(SqlLibraryOperators.DATE_TRUNC, rexBuilder.makeLiteral("hour"), tsRefBigint());

        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragmentWithBucket(dateTrunc), "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.HISTOGRAM, hints.shape());
        assertEquals(FastPathHintSpec.BucketOp.FLOOR_TO_MULTIPLE, hints.histogramBucketOp());
        assertEquals(3_600_000L, hints.histogramBucketOperand());
    }

    /** {@code date_trunc('hour', ts)} on a {@code date_nanos} field scales to nanoseconds. */
    public void testHistogramDateTruncHourOnDateNanos() {
        RexNode dateTrunc = rexBuilder.makeCall(SqlLibraryOperators.DATE_TRUNC, rexBuilder.makeLiteral("hour"), tsRefBigint());

        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragmentWithBucket(dateTrunc), "ts", "date_nanos", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.HISTOGRAM, hints.shape());
        assertEquals(FastPathHintSpec.BucketOp.FLOOR_TO_MULTIPLE, hints.histogramBucketOp());
        assertEquals(3_600_000_000_000L, hints.histogramBucketOperand());
    }

    /** {@code date_trunc('day', ts)} is calendar/timezone dependent => NONE. */
    public void testNoneWhenDateTruncDay() {
        RexNode dateTrunc = rexBuilder.makeCall(SqlLibraryOperators.DATE_TRUNC, rexBuilder.makeLiteral("day"), tsRefBigint());

        FastPathHintSpec hints = FastPathHintExtractor.extract(histogramFragmentWithBucket(dateTrunc), "ts", "date", BACKEND, false);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
    }
}
